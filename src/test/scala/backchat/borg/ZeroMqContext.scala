package backchat.borg

import org.zeromq.ZMQ
import mojolly.io.FreePort
import org.zeromq.ZMQ.{ Poller, Context, Socket }
import org.specs2.specification.Around
import telepathy._
import util.DynamicVariable
import org.specs2.execute.Result
import akka.actor.ActorRef
import akka.zeromq.{ ZMQMessage, Frame }
import collection.mutable.ListBuffer
import akka.testkit.{ TestKit, TestActorRef }

object TestTelepathyServer {
  type ZMessageHandler = Seq[Frame] ⇒ Any
  def newContext = ZMQ.context(1)
  def apply(context: ZMQ.Context, poller: ZeroMQPoller, socketType: Int = ZMQ.XREP, name: String = ""): Server = {
    val socket = context.socket(socketType)
    val port = FreePort.randomFreePort(50)
    val addr = TelepathAddress("127.0.0.1", port)
    if (socketType == ZMQ.SUB) {
      socket.connect(addr.address)
      socket.subscribe("".getBytes(Utf8))
    } else socket.bind(addr.address)
    name.toOption foreach { n ⇒ socket.setIdentity(n.getBytes(Utf8)) }
    socket.setLinger(0L)
    Server(socket, addr, poller, context)
  }

  case class Server(socket: Socket, address: TelepathAddress, poller: ZeroMQPoller, context: ZMQ.Context) {
    def stop = {
      poller -= socket
      socket.close()
      poller.dispose()
    }

    def onMessage(fn: ZMessageHandler) = {
      poller += socket -> fn
    }

    def poll(interval: Duration) = poller poll interval.millis
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
      case null                                   ⇒ noBytes
      case bytes: Array[Byte] if bytes.length > 0 ⇒ bytes
      case _                                      ⇒ noBytes
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

  def size = poller.getSize
  def isEmpty = size > 0
}

trait ZeroMqContext extends Around with TestKit {

  val deser = new BorgZMQMessageSerializer

  val _zmqContext = new DynamicVariable[ZMQ.Context](null)
  def zmqContext = _zmqContext.value

  def around[T](t: ⇒ T)(implicit evidence$1: (T) ⇒ Result) = {
    val ctxt = ZMQ.context(1)
    val res = _zmqContext.withValue(ctxt)(t)
    ctxt.term()
    res
  }

  def withServer[T](socketType: Int = ZMQ.XREP)(fn: TestTelepathyServer.Server ⇒ T)(implicit evidence$1: (T) ⇒ Result): T = {
    require(zmqContext.isNotNull, "Did you wrap the test method with a `this { ... }`?")
    val poller = new ZeroMQPoller(zmqContext)
    val kv = TestTelepathyServer(zmqContext, poller, socketType = socketType)
    val res = fn(kv)
    kv.stop
    res
  }

  def withClient[T](address: TelepathAddress, subscriptionManager: Option[ActorRef] = None)(fn: ActorRef ⇒ T)(implicit evidence$1: (T) ⇒ Result): T = {
    val client = TestActorRef(new Client(TelepathClientConfig(address, subscriptionManager = subscriptionManager))).start()
    val res = fn(client)
    client.stop()
    res
  }

  def zmqMessage(frames: Seq[Frame]) = deser.fromZMQMessage(ZMQMessage(frames.last.payload.toArray).asInstanceOf[ZMQMessage])

}