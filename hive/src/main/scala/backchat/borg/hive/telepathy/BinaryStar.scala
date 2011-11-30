package backchat
package borg
package hive
package telepathy

import akka.actor._
import akka.zeromq._
import borg.BorgMessage.MessageType
import net.liftweb.json._
import telepathy.Messages.{Tell, HiveRequest, Ask}
import java.util.concurrent.TimeUnit
import mojolly.{ScheduledTask, BackchatFormats}
import telepathy.BinaryStar.Voter

object HiveTimer {
  
  private def r(thunk: => Any) = new Runnable {
    def run() { thunk }
  }
  def apply(interval: Duration, receiver: ActorRef,  message: Any): HiveTimer = {
    new HiveTimer(interval, r { receiver ! message }, 0.millis, true)
  }
}
case class HiveTimer(interval: Duration, task: Runnable, initialDelay: Duration = 0.seconds, repeat: Boolean = false) {
  private var _task: ScheduledTask = _
  
  def isActive = _task.isNotNull && _task.isActive
  def isInactive = _task.isNull || _task.isInactive
  
  def start = {
    val fut = if (repeat)
      Scheduler.schedule(task, initialDelay.millis, interval.millis, TimeUnit.MILLISECONDS)
    else 
      Scheduler.scheduleOnce(task, interval.millis, TimeUnit.MILLISECONDS)
    _task = ScheduledTask(fut)
  }
  
  def stop = {
    if (isActive) _task.stop()
  }
  
  def restart = {
    stop
    start
  }
}

case class BinaryStarConfig(
             startAs: BinaryStar.BinaryStarState,
             frontend: TelepathAddress,
             statePub: TelepathAddress,
             stateSub: TelepathAddress,
             listener: Option[ActorRef] = None,
             heartbeat: Duration = 1.second)

object BinaryStar {
  
  trait Handler { self: Actor with Logging =>
    private var _isActive = false
    protected def isActive = _isActive
    
    protected def handleBStarMessage: Receive = {
      case Active => {
        if (!_isActive) {
          logger info "We just became master, activating server"
          _isActive = true
          onActivate()
          become(handleBStarMessage orElse receiveRequest, true)
        }
      }
      case Passive => {
        if (_isActive) {
          logger info "We just became a slave, deactivating server"
          become(handleBStarMessage, true)
          onDeactivate()
        }
      }
    }
    
    protected def receiveRequest: Receive
    
    protected def onActivate() {}
    protected def onDeactivate() {}
  }
  
  sealed trait BinaryStarState {

    override def toString = { getClass.getSimpleName split "\\$" filter (_.nonEmpty) last }

  }
  sealed trait BinaryStarRole extends BinaryStarState
  case object Primary extends BinaryStarRole
  case object Backup extends BinaryStarRole
  case object Active extends BinaryStarState
  case object Passive extends BinaryStarState
  case object Error extends BinaryStarState

  object Messages {

    sealed trait BinaryStarMessage extends BorgMessageWrapper
    sealed trait BinaryStarEvent extends BinaryStarMessage {
      def symbol: Symbol
      def unwrapped = BorgMessage(MessageType.System, "", ApplicationEvent(symbol))
    }
    abstract class BinaryStarEventImpl(val symbol: Symbol) extends BinaryStarEvent
    case object PeerPrimary extends BinaryStarEventImpl('primary)
    case object PeerBackup extends BinaryStarEventImpl('backup)
    case object PeerActive extends BinaryStarEventImpl('active)
    case object PeerPassive extends BinaryStarEventImpl('passive)
    
    sealed trait BinaryStarControlMessage
    case object Heartbeat extends BinaryStarControlMessage
    case class ClientRequest(request: BorgMessageWrapper) extends BinaryStarControlMessage 

    def apply(msg: BorgMessage) = msg match {
      case BorgMessage(MessageType.System, _, ApplicationEvent('primary, _), _, _) => PeerPrimary
      case BorgMessage(MessageType.System, _, ApplicationEvent('backup, _), _, _) => PeerBackup
      case BorgMessage(MessageType.System, _, ApplicationEvent('active, _), _, _) => PeerActive
      case BorgMessage(MessageType.System, _, ApplicationEvent('passive, _), _, _) => PeerPassive
    }
  }
  
  class BinaryStarDeserializer extends BorgZMQMessageSerializer {
    override def apply(frames: Seq[Frame]) = {
      BinaryStar.Messages(fromZMQMessage(ZMQMessage(frames)))
    }
  }
  
  class Voter(reactor: ActorRef) extends Actor with Logging {
    val serializer = new BorgZMQMessageSerializer
    protected def receive = {
      case Connecting | Closed =>
      case m: ZMQMessage => {
        logger debug "voter received event"
        val msg = telepathy.Messages(serializer fromZMQMessage m)
        reactor ! Messages.ClientRequest(msg)
      }
    }
  }
  
//  class Reactor(fsm: ActorRef) extends Telepath {
//    protected def receive = {
//      case _ =>
//    }
//  }
  
  /*
   * The binary star reactor links several components.
   * It takes a listener representing the server interface which will respond to cluster events
   * It has:
   *   - a frontend socket (the actual server socket for clients)
   *   - a state publisher socket (PUB)
   *   - a state subscriber socket (SUB)
   */
  class Reactor(config: BinaryStarConfig) extends Telepath with FSM[BinaryStarState, Unit] {
    import FSM._
    import Messages._

    var nextPeerExpiry = schedulePeerExpiry
    def schedulePeerExpiry = System.currentTimeMillis + (2 * config.heartbeat.millis)
    
    val deserializer = new BinaryStarDeserializer
    val statePub: ActorRef = newSocket(SocketType.Pub, MessageDeserializer(deserializer), SocketListener(stateListener))
    val stateSub: ActorRef = newSocket(SocketType.Sub, MessageDeserializer(deserializer), SocketListener(stateListener))
    val voter: ActorRef = newSocket(SocketType.Router, NoLinger, SocketListener(voterListener))

    private lazy val voterListener = {
      val l = Actor.actorOf(new Voter(self))
      self startLink l
      l
    }
    
    private lazy val stateListener = {
      val parent = self
      val l = Actor.actorOf(new Actor {
        def receive = {
          case Connecting | Closed =>
          case m: BinaryStarEvent => {
            logger debug "state listener received an event: %s".format(m)
            nextPeerExpiry = schedulePeerExpiry
            parent ! m
          }
        }
      })
      self startLink l
      l
    }
    
    startWith(config.startAs, ())

    when(Primary) {
      case Ev(PeerBackup) => {
        logger info "Connected to backup (slave), ready as master"
        config.listener foreach { _ ! Active }
        goto(Active)
      }
      case Ev(PeerActive) => {
        logger info "Connected to backup (master), ready as slave"
        config.listener foreach { _ ! Passive }
        goto(Passive)
      }
      case Ev(ClientRequest(request)) => {
        logger info "Request from client, ready as master"
        config.listener foreach { _ ! Active }
        config.listener foreach { _ forward request }
        goto(Active)
      }
      case Ev(Heartbeat) => {
        logger debug "[%s] Received heartbeat command".format(stateName)
        statePub ! deserializer.toZMQMessage(PeerPrimary.unwrapped)
        stay
      }
    }

    when(Backup) {
      case Ev(PeerActive) => {
        logger info "Connected to primary (master), ready as slave"
        config.listener foreach { _ ! Passive }
        goto(Passive)
      }
      case Ev(ClientRequest(request)) => { // perhaps forward to master?
        stay
      }
      case Ev(Heartbeat) => {
        logger debug "[%s] Received heartbeat command".format(stateName)
        statePub ! deserializer.toZMQMessage(PeerBackup.unwrapped)
        stay
      }
    }

    when(Active) {
      case Ev(PeerActive) => {
        logger error "We have dual masters, confused"
        goto(Error)
      }
      case Ev(Heartbeat) => {
        logger debug "[%s] Received heartbeat command".format(stateName)
        statePub ! deserializer.toZMQMessage(PeerActive.unwrapped)
        stay
      }
    }

    when(Passive) {
      case Ev(PeerPrimary) => {
        logger info "Primary (master) is restarting, becoming master"
        goto(Active)
      }
      case Ev(PeerBackup) => {
        logger info "Backup (slave) is restarting, becoming active"
        goto(Active)
      }
      case Ev(PeerPassive) => {
        logger error "We have dual slaves, confused"
        stay
      }
      case Ev(ClientRequest(request)) => {
        require(nextPeerExpiry > 0)
        if (System.currentTimeMillis >= nextPeerExpiry) {
          logger info "Failover succeeded, ready as master"
          goto(Active)
        } else {
          goto(Error)
        }
      }
      case Ev(Heartbeat) => {
        logger debug "[%s] Received heartbeat command".format(stateName)
        statePub ! deserializer.toZMQMessage(PeerPassive.unwrapped)
        stay
      }
    }
    
    onTransition {
      case Passive -> Active => {
        config.listener foreach { _ ! Active }
      }
    }
    
    whenUnhandled {
      case Ev('init) => {
        voter ! Bind(config.frontend.address)
        stateSub ! Connect(config.stateSub.address)
        statePub ! Bind(config.statePub.address)
        setTimer("heartbeat", Heartbeat, akka.util.Duration(1000L, TimeUnit.MILLISECONDS), true)
        stay
      }
    }
    
    when(Error) {
      case Ev(a) => {
        self.stop
        stay
      }
    }
    
    initialize

    override def preStart() {
      super.preStart()
      self ! 'init
    }
  }
}