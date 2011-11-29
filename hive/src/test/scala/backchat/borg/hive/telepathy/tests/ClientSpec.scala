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
import akka.zeromq.{ZMQMessage, Frame}
import org.specs2.execute.Result
import BorgMessage.MessageType
import telepathy.Messages._
import akka.actor._
import mojolly.testing.{MojollySpecification}
import org.specs2.specification.{Around, After, Step, Fragments}
import util.DynamicVariable

object TestTelepathyServer {
  type ZMessageHandler = Seq[Frame] ⇒ Any
  def newContext = ZMQ.context(1)
  def apply(context: ZMQ.Context, poller: ZeroMQPoller, socketType: Int = ZMQ.XREP, name: String = ""): Server = {
    val socket = context.socket(socketType)
    val port = FreePort.randomFreePort(50)
    val addr = TelepathAddress("127.0.0.1", Some(port))
    socket.bind(addr.address)
    name.toOption foreach { n => socket.setIdentity(n.getBytes(Utf8)) }
    socket.setLinger(0L)
    Server(socket, addr, poller, context)
  }
  
  case class Server(socket: Socket,  address: TelepathAddress, poller: ZeroMQPoller, context: ZMQ.Context) {    
    def stop = {
      poller -= socket
      socket.close()
      poller.dispose()
    }
    
    def onMessage(fn: ZMessageHandler) = {
      poller += socket -> fn
    }

    def poll(timo: Long = -1) = poller.poll(timo)
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

trait ZeroMqSpecification extends MojollySpecification {
  
  val zmqContext = ZMQ.context(1)
  val poller = new ZeroMQPoller(zmqContext)
  
//  override def map(fs: => Fragments) = super.map(fs) ^ Step(poller.dispose()) ^ Step(zmqContext.term())
}
trait ZeroMqContext extends Around {
  
  val deser = new BorgZMQMessageSerializer

  val _zmqContext = new DynamicVariable[ZMQ.Context](null)
  def zmqContext = _zmqContext.value

  def around[T](t: => T)(implicit evidence$1: (T) => Result) = {
    val ctxt = ZMQ.context(1)
    val res = _zmqContext.withValue(ctxt)(t)
    ctxt.term()
    res
  }

  def withServer[T](socketType: Int = ZMQ.XREP)(fn: TestTelepathyServer.Server => T)(implicit evidence$1: (T) => Result): T = {
    require(zmqContext.isNotNull)
    val poller = new ZeroMQPoller(zmqContext)    
    val kv = TestTelepathyServer(zmqContext, poller, socketType = socketType)
    val res = fn(kv)
    kv.stop
    res
  }

  def withClient[T](address: TelepathAddress)(fn: ActorRef => T)(implicit evidence$1: (T) => Result): T = {
    val client = newTelepathicClient(address.address)
    val res = fn(client)
    client.stop()
    res
  }

  def zmqMessage(frames: Seq[Frame]) = deser.fromZMQMessage(ZMQMessage(frames.last.payload.toArray).asInstanceOf[ZMQMessage])

}

class ClientSpec extends MojollySpecification { def is =
  "A telepathic client should" ^
    "when responding to messages" ^
      "handle an enqueue message" ! context.handlesEnqueue ^
      "handle a request message" ! context.handlesRequest ^ end
  
  def context = new ClientSpecContext
  
  class ClientSpecContext extends ZeroMqContext {
    def handlesEnqueue = this {
      withServer() { server =>
        withClient(server.address) { client =>
          val appEvt = ApplicationEvent('pingping, JArray(JString("the message") :: Nil))
          val latch = new StandardLatch
  
          server onMessage { (frames: Seq[Frame]) =>
            val msg = zmqMessage(frames)
            if (msg.target == "target" && msg.payload == appEvt) {
              latch.open()
            }
          }
  
          client ! Tell("target", appEvt)
          server.poll(2000)
          latch.tryAwait(2, TimeUnit.SECONDS) must beTrue
        }
      }
    }
    
    def handlesRequest = this {
      withServer() { server =>
        withClient(server.address) { client =>
          val appEvt = ApplicationEvent('pingping, JArray(JString("the message") :: Nil))
          val appEvt2 = ApplicationEvent('pongpong, JArray(JString("the response message") :: Nil))
  
          server onMessage { (frames: Seq[Frame]) =>
            val msg = zmqMessage(frames)
            println("server received: %s" format msg)
            if (msg.target == "target" && msg.payload == appEvt && msg.messageType == MessageType.RequestReply) {
              server.socket.send(frames.head.payload.toArray, ZMQ.SNDMORE)
              server.socket.send(Messages(msg).asInstanceOf[Ask].respond(appEvt2).toBytes, 0)
            }
          }
  
          val req = client ? Ask("target", appEvt)
          server.poll(2000)
          req.as[ApplicationEvent] must beSome(appEvt2)
        }
      }
    }
  }
  
  
}