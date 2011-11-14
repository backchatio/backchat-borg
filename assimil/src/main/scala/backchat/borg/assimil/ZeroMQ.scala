package backchat
package borg
package assimil

import akka.dispatch.{ Dispatchers }
import collection.mutable.ListBuffer
import java.util.concurrent.{ TimeUnit, Future, Executors }
import org.zeromq.{ ZMQ ⇒ JZMQ }
import org.zeromq.ZMQ.{ Poller }
import akka.agent.Agent
import akka.actor._
import akka.config.Supervision._

object MessageType extends Enumeration {
  type MessageType = Value
  val System = Value("s")
  val RequestReply = Value("rr")
  val Enqueue = Value("q")
  val PubSub = Value("ps")
}

trait ZeroMQDevicePart {
  def deviceName: String

  protected def context: Context
  protected lazy val poller = new ZeroMQ.ZeroMQPoller(context)

  def init()
  def dispose()
  def send(zmsg: ZMessage)
  def execute(): Boolean

}

trait ZeroMQDevice extends ZeroMQDevicePart with Logging {

  //  protected lazy val poller = new ZeroMQ.ZeroMQPoller(context)
  protected var keepRunning = true

  protected def stop() {
    keepRunning = false
  }

  def dispose() {
    logger info ("Stopping: %s" format id.name)
    poller.dispose()
  }

  def init() {
    logger info ("Starting: %s" format id.name)
    poller.init()
  }

  def send(zmsg: ZMessage) {}

  def execute(): Boolean = {
    poller.poll()
    keepRunning
  }

  def id: Symbol
  def onError(th: Throwable) {}
}

trait PingPongObserver extends Actor { this: Actor ⇒

  become(observeClientPing orElse receive, false)

  protected def observeClientPing: Receive = {
    case ClientPing(client) ⇒ onClientPing(client)
  }

  protected def onClientPing(client: Array[Byte])
}

object ZeroMQ extends Logging {

  private[borg] val supervisor = Actor.actorOf(new Actor {
    self.faultHandler = OneForOneStrategy(List(classOf[Throwable]), 5, 5000)

    protected def receive: Receive = { case _ ⇒ }

    override def postStop() {
      val i = self.linkedActors.values.iterator
      while (i.hasNext) {
        val ref = i.next
        ref.stop()
        self.unlink(ref)
      }
    }
  }).start()

  private var activeDevices = Set[ZeroMQHostedDevice]()
  //  private val BridgeDispatcher = Dispatchers.newHawtDispatcher(false)

  type ZMessageHandler = ZMessage ⇒ Unit
  private val _context = new Agent[Context](null)
  def context = {
    _context()
  }
  def start(ioThreads: Int = 1) {
    logger info "Starting global ∅MQ"
    if (_context() == null) {
      _context.send(JZMQ context ioThreads)
    }
  }

  class ZeroMQPoller(context: Context) {
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
          pollinHandlers(idx)(ZMessage(poller.getSocket(idx)))
        }
      }
    }

    def size = poller.getSize
    def isEmpty = size > 0
  }

  def bridgeDispatcher(actor: ActorRef) {
    actor.dispatcher = Dispatchers.newThreadBasedDispatcher(actor)
  }

  private val executor = Executors.newCachedThreadPool

  trait ZeroMQHostedDevice {
    def start()
    def restart()
    def stop()
  }

  private def r(a: ⇒ Unit) = new Runnable { def run() { a } }

  private class DeviceHost(device: ZeroMQDevice) extends ZeroMQHostedDevice with Logging {

    @volatile
    private var future: Future[_] = null

    def isRunning = future != null && !future.isDone

    private def logged(act: ⇒ Any) = {
      try {
        act
        true
      } catch {
        case th ⇒ {
          logger error ("There was an error in the ZeroMQ device [%s].".format(device.id.name), th)
          device.onError(th)
          false
        }
      }
    }

    def start() {
      if (!isRunning) {
        future = executor.submit(r {
          try {
            var keepGoing = logged { device.init() }
            while (keepGoing) { keepGoing = !Thread.interrupted && logged { device.execute() } }
          } catch {
            case e: InterruptedException ⇒ {} // we expect this
          } finally {
            try { device.dispose() } catch {
              case e ⇒ {
                logger warn ("There was an error while shutting down the device: %s" format device.deviceName, e)
                throw e
              }
            }
          }
        })
      }
      this
    }

    def stop() {
      if (isRunning) {
        future.cancel(true)
      }
      this
    }

    def restart() {
      stop()
      start()
    }
  }

  def startDevice(zmqDevice: ZeroMQDevice): ZeroMQHostedDevice = {
    val deviceHost = new DeviceHost(zmqDevice)
    deviceHost.start()
    activeDevices += deviceHost
    deviceHost
  }

  def stop() {
    activeDevices foreach { _.stop() }
    executor.shutdown()
    executor.awaitTermination(5, TimeUnit.SECONDS)
    //    context.term()
    _context.send(null.asInstanceOf[Context])
    _context.close()
  }

}