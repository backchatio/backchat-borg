package backchat
package borg
package hive
package telepathy
package tests

import org.zeromq.ZMQ
import mojolly.io.FreePort
import org.zeromq.ZMQ.{Context, Poller, Socket}
import collection.mutable.ListBuffer
import net.liftweb.json.JsonAST.{JArray, JString}
import org.multiverse.api.latches.StandardLatch
import java.util.concurrent.TimeUnit
import org.specs2.specification.{Fragments, Step}
import akka.zeromq.{ZMQMessage, Frame}
import org.specs2.execute.Result
import BorgMessage.MessageType
import telepathy.Messages._
import akka.actor._
import mojolly.testing.{MojollySpecification, AkkaSpecification}

object TestTelepathyServer {
  def newContext = ZMQ.context(1)
  def apply(context: ZMQ.Context): (Socket, TelepathAddress) = {
    val socket = context.socket(ZMQ.XREP)
    val port = FreePort.randomFreePort(50)
    val addr = TelepathAddress("127.0.0.1", port)
    socket.bind(addr.address)
    socket.setLinger(0L)
    (socket, addr)
  }
}

class ZeroMQPoller(context: Context) {
  type ZMessageHandler = Seq[Frame] ⇒ Any
  private var poller: Poller = null
  private val pollinHandlers = ListBuffer[ZMessageHandler]()
  private val sockets = ListBuffer[Socket]()

  protected def register(socket: Socket, messageHandler: ZMessageHandler) {
    sockets += socket
    pollinHandlers += messageHandler
    if (poller != null) {
      poller.register(socket, Poller.POLLIN)
    }
  }

  def init() {
    poller = context.poller(sockets.size)
    sockets.foreach(poller.register(_, Poller.POLLIN))
  }

  def dispose() {
    sockets.foreach(poller.unregister(_))
    sockets.clear()
    pollinHandlers.clear()
  }

  def -=(socket: Socket) {
    val idx = sockets indexOf socket
    poller unregister socket
    sockets -= socket
    pollinHandlers remove idx
  }

  def +=(handler: (Socket, ZMessageHandler)) {
    (register _).tupled(handler)
  }

  def poll(timeout: Long = -1) {
    if (poller == null) init()
    val timo = if (timeout > 0) timeout * 1000 else timeout
    poller.poll(timo)
    (0 until poller.getSize) foreach { idx ⇒
      if (poller.pollin(idx)) {
        pollinHandlers(idx)(receiveFrames(poller.getSocket(idx)))
      }
    }
  }
  private val noBytes = Array.empty[Byte]
  private def receiveFrames(socket: Socket): Seq[Frame] = {
    @inline def receiveBytes(): Array[Byte] = socket.recv(0) match {
      case null => noBytes
      case bytes: Array[Byte] if bytes.length > 0 => bytes
      case _ => noBytes
    }
    receiveBytes() match {
      case `noBytes` => Vector.empty
      case someBytes =>
        var frames = Vector(Frame(someBytes))
        while (socket.hasReceiveMore) receiveBytes() match {
          case `noBytes` =>
          case someBytes => frames :+= Frame(someBytes)
        }
        frames
    }
  }

  def size = poller.getSize
  def isEmpty = size > 0
}

class ClientSpec extends MojollySpecification { def is =
  "A telepathic client should" ^
    "when responding to messages" ^
      "handle an enqueue message" ! handlesEnqueue ^
      "handle a request message" ! handlesRequest ^ end
  
  val deser = new BorgZMQMessageSerializer

  def withContext[T](fn: (Context, ZeroMQPoller) => T)(implicit evidence$1: (T) => Result): T = {
    val ctxt = TestTelepathyServer.newContext
    val poller = new ZeroMQPoller(ctxt)
    val rs = fn(ctxt, poller)
    ctxt.term
    rs
  }
  
  def withServer[T](ctxt: Context, poller: ZeroMQPoller)(fn: (Socket, TelepathAddress) => T)(implicit evidence$1: (T) => Result): T = {
    val kv = TestTelepathyServer(ctxt)
    val res = fn.tupled.apply(kv)
    poller.dispose()
    kv._1.close()
    res
  }
  
  def withClient[T](address: TelepathAddress)(fn: ActorRef => T)(implicit evidence$1: (T) => Result): T = {
    val client = newTelepathicClient(address.address)
    val res = fn(client)
    client.stop()
    res
  }
  
  def handlesEnqueue = {
    withContext { (context, poller) =>
      withServer(context, poller) { (server, address) =>
        withClient(address) { client => 
          val appEvt = ApplicationEvent('pingping, JArray(JString("the message") :: Nil))
          val latch = new StandardLatch
          
          poller += (server -> ((frames: Seq[Frame]) => {
            val msg = deser.fromZMQMessage(ZMQMessage(frames.last.payload.toArray).asInstanceOf[ZMQMessage])
            if (msg.target == "target" && msg.payload == appEvt) {
              latch.open()
            }
          }))
      
          client ! Tell("target", appEvt)
          poller.poll(5000)
          latch.tryAwait(2, TimeUnit.SECONDS) must beTrue
        }
      }
    }
  }
  
  def handlesRequest = {
    withContext { (context, poller) =>
      withServer(context, poller) { (server, address) =>
        withClient(address) { client =>
          val appEvt = ApplicationEvent('pingping, JArray(JString("the message") :: Nil))
          val appEvt2 = ApplicationEvent('pongpong, JArray(JString("the response message") :: Nil))

          poller += (server -> ((frames: Seq[Frame]) => {
            val msg = deser.fromZMQMessage(ZMQMessage(frames.last.payload.toArray).asInstanceOf[ZMQMessage])
            if (msg.target == "target" && msg.payload == appEvt && msg.messageType == MessageType.RequestReply) {
              server.send(frames.head.payload.toArray, ZMQ.SNDMORE)
              server.send(Messages(msg).asInstanceOf[Ask].respond(appEvt2).toBytes, 0)
            }}))
          
          var res: Option[ApplicationEvent] = None
          val subj = Actor.actorOf(new Actor {
            def receive = {
              case 'makeRequest => {
                val req = client ? Ask("target", appEvt)
                res = Some(req.as[ApplicationEvent].get)
              }
            }
          }).start()
          
          subj ! 'makeRequest
          poller.poll(5000)
          subj.stop()
          res must be_==(Some(appEvt2)).eventually
        }
      }
    }
  }
}