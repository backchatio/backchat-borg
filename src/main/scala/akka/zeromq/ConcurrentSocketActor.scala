/**
 * Copyright (C) 2009-2011 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.zeromq

import akka.actor.Actor
import org.zeromq.ZMQ.{ Socket, Poller }
import org.zeromq.{ ZMQ ⇒ JZMQ }
import java.nio.charset.Charset
import akka.dispatch.{ Future, MessageDispatcher }
import akka.event.EventHandler

private[zeromq] class ConcurrentSocketActor(params: SocketParameters, dispatcher: MessageDispatcher) extends Actor {

  private val noBytes = Array[Byte]()
  private val socket: Socket = params.context.socket(params.socketType)
  private val poller: Poller = params.context.poller

  self.dispatcher = dispatcher

  protected def receive: Receive = {
    case Send(frames) ⇒
      sendFrames(frames)
      pollAndReceiveFrames()
    case ZMQMessage(frames) ⇒
      sendFrames(frames)
      pollAndReceiveFrames()
    case Connect(endpoint) ⇒
      socket.connect(endpoint)
      notifyListener(Connecting)
    case Bind(endpoint) ⇒
      socket.bind(endpoint)
    case Subscribe(topic) ⇒
      socket.subscribe(topic.toArray)
    case Unsubscribe(topic) ⇒
      socket.unsubscribe(topic.toArray)
  }

  override def preStart {
    configureSocket(socket)
    poller.register(socket, Poller.POLLIN)
  }

  override def postStop {
    poller.unregister(socket)
    socket.close
    notifyListener(Closed)
  }

  private def sendFrames(frames: Seq[Frame]) {
    def sendBytes(bytes: Seq[Byte], flags: Int) {
      socket.send(bytes.toArray, flags)
    }
    val iter = frames.iterator
    while (iter.hasNext) {
      val payload = iter.next.payload
      val flags = if (iter.hasNext) JZMQ.SNDMORE else 0
      sendBytes(payload, flags)
    }
  }

  private var currentPoll: Option[Future[Boolean]] = None
  private def pollAndReceiveFrames() {
    currentPoll = currentPoll orElse Some(newEventLoop)
  }

  private def newEventLoop =
    Future {
      poller.poll(params.pollTimeoutDuration.toMillis) > 0 && poller.pollin(0)
    } onComplete { fut ⇒
      if (fut.get) {
        receiveFrames() match {
          case Seq()  ⇒
          case frames ⇒ notifyListener(params.deserializer(frames))
        }
      }
      reschedule()
    } onException {
      case ex ⇒ {
        EventHandler.error(ex, this, "There was an error receiving messages on the zeromq socket")
        reschedule()
      }
    }

  private def reschedule() {
    currentPoll = None
    pollAndReceiveFrames()
  }

  private def receiveFrames(): Seq[Frame] = {
    @inline def receiveBytes(): Array[Byte] = socket.recv(0) match {
      case null | `noBytes`   ⇒ noBytes
      case bytes: Array[Byte] ⇒ bytes
    }
    receiveBytes() match {
      case `noBytes` ⇒ Vector.empty
      case someBytes ⇒
        var frames = Vector(Frame(someBytes))
        while (socket.hasReceiveMore) receiveBytes() match {
          case `noBytes` ⇒
          case bytes     ⇒ frames :+= Frame(bytes)
        }
        frames
    }
  }

  private def notifyListener(message: Any) {
    params.listener foreach { listener ⇒
      if (listener.isShutdown)
        self.stop
      else
        listener ! message
    }
  }

  val Utf8 = Charset.forName("UTF-8")
  private def configureSocket(sock: Socket) {
    params.options foreach {
      case lo: LingerOption   ⇒ sock setLinger lo.value
      case HWM(value)         ⇒ sock setHWM value
      case Affinity(value)    ⇒ sock setAffinity value
      case Rate(value)        ⇒ sock setRate value
      case RecoveryIVL(value) ⇒ sock setRecoveryInterval value
      case SndBuf(value)      ⇒ sock setSendBufferSize value
      case RcvBuf(value)      ⇒ sock setReceiveBufferSize value
      case Identity(value)    ⇒ sock setIdentity value.getBytes(Utf8)
      case McastLoop(value)   ⇒ sock setMulticastLoop value
    }
  }
}
