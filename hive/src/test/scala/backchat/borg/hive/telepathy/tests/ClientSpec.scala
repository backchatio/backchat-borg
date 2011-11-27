package backchat
package borg
package hive
package telepathy
package tests

import mojolly.testing.AkkaSpecification
import org.zeromq.ZMQ
import mojolly.io.FreePort
import org.zeromq.ZMQ.{Context, Poller, Socket}
import akka.zeromq.{Frame, ZeroMQ, SocketType}
import akka.zeromq.Frame._
import collection.mutable.ListBuffer
import net.liftweb.json.JsonAST.{JArray, JString}
import org.multiverse.api.latches.StandardLatch
import java.util.concurrent.TimeUnit
import telepathy.HiveRequests.Tell
import org.specs2.specification.{Fragments, Step, Fragment}

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
  type ZMessageHandler = Seq[Frame] ⇒ Unit
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

class ClientSpec extends AkkaSpecification { def is = 
  "A telepathic client should" ^
    "when responding to messages" ^
      "handle an enqueue message" ! handlesEnqueue ^
      "handle a request message" ! handlesRequest ^ end
  
  val context = TestTelepathyServer.newContext
  val deser = new BorgZMessageDeserializer
  
  override def map(fs: => Fragments) =  super.map(fs) ^ Step(context.term())

  def handlesEnqueue = {
    val (server, address) = TestTelepathyServer(context)
    val client = newTelepathicClient(address.address)
    val appEvt = ApplicationEvent('pingping, JArray(JString("the message") :: Nil))
    val poller = new ZeroMQPoller(context)
    val latch = new StandardLatch
    poller += (server -> ((frames: Seq[Frame]) => {
      val msg = deser(frames).asInstanceOf[BorgMessage]
      if (msg.target == "target" && msg.payload == appEvt) {
        latch.open()
      }
    }))

    client ! Tell("target", appEvt)
    poller.poll(5000)
    val res = latch.tryAwait(2, TimeUnit.SECONDS) must beTrue
    poller.dispose()
    client.stop()
    server.close()
    res
  }
  
  def handlesRequest = pending
}