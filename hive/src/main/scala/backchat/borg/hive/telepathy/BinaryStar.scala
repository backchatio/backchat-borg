package backchat
package borg
package hive
package telepathy

import akka.actor._
import akka.zeromq._
import akka.dispatch.CompletableFuture
import org.zeromq.ZMQ
import borg.BorgMessage.MessageType
import mojolly.{BackchatFormats, LibraryImports}
import net.liftweb.json._
import telepathy.Messages.{Tell, HiveRequest, Ask, HiveMessage}
import java.util.concurrent.TimeUnit
import telepathy.BinaryStar.BinaryStarDeserializer

case class BinaryStarConfig(
             startAs: BinaryStar.BinaryStarRole,
             local: TelepathAddress,
             remote: TelepathAddress,
             listener: Option[ActorRef],
             heartbeat: Period = 2.seconds,
             peerExpiry: Period = 1.second)

object BinaryStar {

  implicit val formats = new BackchatFormats {
    override val typeHints = ShortTypeHints(List(classOf[Ask], classOf[Tell]))
  }
  
  sealed trait BinaryStarHandler
  case class OnNewMaster(actor: ActorRef) extends BinaryStarHandler
  case class OnNewSlave(actor: ActorRef) extends BinaryStarHandler
  case class VoterHandler(actor: ActorRef) extends BinaryStarHandler
  
  sealed trait BinaryStarState
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
      def unwrapped = BorgMessage(MessageType.System, null, ApplicationEvent(symbol))
    }
    private abstract class BinaryStarEventImpl(val symbol: Symbol) extends BinaryStarEvent
    case object PeerPrimary extends BinaryStarEventImpl('master)
    case object PeerBackup extends BinaryStarEventImpl('backup)
    case object PeerActive extends BinaryStarEventImpl('active)
    case object PeerPassive extends BinaryStarEventImpl('passive)
    case class ClientRequest(request: HiveRequest) extends BinaryStarMessage {
      def unwrapped = BorgMessage(MessageType.System, "", ApplicationEvent('client_request, request.toJValue))
    }
    case class Heartbeat(millis: BigInt) extends BinaryStarMessage {
      def unwrapped = BorgMessage(MessageType.System, "", ApplicationEvent('heartbeat, JInt(millis)))
    }

    def apply(msg: BorgMessage) = msg match {
      case BorgMessage(MessageType.System, _, ApplicationEvent('master, _), _, _) => PeerPrimary
      case BorgMessage(MessageType.System, _, ApplicationEvent('backup, _), _, _) => PeerBackup
      case BorgMessage(MessageType.System, _, ApplicationEvent('active, _), _, _) => PeerActive
      case BorgMessage(MessageType.System, _, ApplicationEvent('passive, _), _, _) => PeerPassive
      case BorgMessage(MessageType.System, _, ApplicationEvent('client_request, req), _, _) => {
        ClientRequest(req.extract[HiveRequest])
      }
      case BorgMessage(MessageType.System, _, ApplicationEvent('heartbeat, JInt(ts)), _, _) => {
        Heartbeat(ts)
      }
    }
  }
  
  class BinaryStarDeserializer extends BorgZMQMessageSerializer {
    def apply(frames: Seq[Frame]) = {
      BinaryStar.Messages(fromZMQMessage(ZMQMessage(frames)))
    }
  }
  
  class Reactor(config: BinaryStarConfig) extends Telepath with FSM[BinaryStarState, Unit] {
    import FSM._
    import Messages._
    import config._

    var masterHandler: Option[ActorRef] = None
    var slaveHandler: Option[ActorRef] = None

    val deserializer = new BinaryStarDeserializer
    val statePub: ActorRef = newSocket(SocketType.Pub, MessageDeserializer(deserializer))
    val stateSub = newSocket(SocketType.Sub, MessageDeserializer(deserializer))
    
    startWith(startAs, ())
    val heartbeatInterval = akka.util.Duration(heartbeat.getMillis, TimeUnit.MILLISECONDS)
    setTimer("heartbeat", Heartbeat(BigInt(System.currentTimeMillis())), heartbeatInterval, true)

    when(Primary) {
      case Event(PeerBackup) => {
        logger info ("Connected to backup (slave), ready as master")
        listener foreach { _ ! Active }
        goto(Active)
      }
      case Event(PeerActive) => goto(Passive)
      case Event(ClientRequest(request)) => {
        masterHandler foreach { _ ! request }
        goto(Active)
      }
    }

    when(Backup) {
      case Event(PeerActive) => {
        logger info "Connected to primary (master), ready as slave"
        goto(Passive)
      }
      case Event(ClientRequest(request)) => { // perhaps forward to master?
        goto(Error)
      }
    }

    when(Active) {
      case Event(PeerActive) => goto(Error)
    }

    when(Passive) {
      case Event(PeerPrimary) => {
        logger info "Primary (master) is restarting, becoming master"
        goto(Active)
      }
      case Event(PeerBackup) => {
        logger info "Backup (slave) is restarting, becoming active"
        goto(Active)
      }
      case Event(PeerPassive) =>
      case Event(ClientRequest(request)) =>
    }
    
    whenUnhandled {
      case Event(Heartbeat(millis)) => {
        stay
      }
    }
    
    when(Error) {
      case Event(a) => {
        self.stop
        stay
      }
    }
    
    initialize
  }
}