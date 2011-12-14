package backchat
package borg
package telepathy

import Messages._
import akka.actor._
import Actor._
import akka.zeromq._
import akka.dispatch.{Future}
import telepathy.Subscriptions.PublishTo

case class ServerConfig(
                         listenOn: TelepathAddress,
                         socket: Option[ActorRef] = None,
                         remoteSubscriptions: Option[ActorRef] = None,
                         localSubscriptions: Option[ActorRef] = None)

object ClientSession {
  def apply(request: ZMQMessage): ClientSession = ClientSession(request.frames.dropRight(1))
}

case class ClientSession(clientId: Seq[Frame], lastSeen: DateTime = DateTime.now)

object Server {
  type Respond = PartialFunction[BorgMessageWrapper, Future[Any]]
}

class Server(config: ServerConfig) extends Telepath {

  import Server._

  lazy val socket = config.socket getOrElse newSocket(SocketType.Router, Linger(0L))
  lazy val remoteSubscriptions = config.remoteSubscriptions getOrElse newRemoteSubscriptions
  lazy val localSubscriptions = config.localSubscriptions getOrElse newLocalSubscriptions
  var activeClients = Vector.empty[ClientSession]

  self.id = config.listenOn.address

  override def preStart() {
    self ! Init
  }

  protected def receive = connectionManagement

  protected def connectionManagement: Receive = {
    case Init ⇒ {
      socket ! Bind(self.id)
      logger info "Server %s is ready".format(self.id)
    }
    case ExpireClients => expireClients()
    case request: ZMQMessage if responseFor(request).isDefinedAt(deserialize(request)) ⇒
      (deserialize _ andThen hugClientIfInNeed(request) andThen responseFor(request))(request) onResult {
        case response: BorgMessageWrapper ⇒ socket ! mkReply(request, response)
        case _ ⇒ // ignore the other ones
      }
    case m: Listen ⇒ {
      localSubscriptions !(m, self.channel)
    }
    case m: Deafen ⇒ {
      localSubscriptions !(m, self.channel)
    }
    case m: Shout ⇒ {
      Vector(remoteSubscriptions, localSubscriptions) foreach {
        _ ! m
      }
    }
    case PublishTo(subscription, topic, payload) ⇒ {
      socket ! Send(subscription.addresses :+ serialize(Shout(topic, payload)))
    }
  }
  
  protected def expireClients() {
    val expired = activeClients filter (_.lastSeen < 2.minutes.ago)
    if (expired.nonEmpty) {
      activeClients = activeClients filterNot activeClients.contains 
      remoteSubscriptions ! ExpireClients(expired)
    }
  }

  protected def addToClients(session: ClientSession) = {
    if (!(activeClients contains session))
      activeClients :+= session
  }

  protected def slideTimeout(session: ClientSession) {
    val withClientId = (cl: ClientSession) => cl.clientId == session.clientId
    val ac = if (activeClients.size > 500) activeClients.par else activeClients
    if (ac exists withClientId) {
      activeClients = Vector(((ac filter withClientId) :+ session): _*)
    } else addToClients(session)
  }

  def hugClientIfInNeed(request: ZMQMessage): PartialFunction[BorgMessageWrapper, BorgMessageWrapper] = {
    case m: HiveRequest ⇒ {
      if (activeClients contains ClientSession(request)) socket ! Hug(m.ccid)
      m
    }
    case m ⇒ m
  }

  protected def responseFor(request: ZMQMessage): Respond = {
    case Ping ⇒ Future {
      slideTimeout(ClientSession(request))
      Pong
    }
    case a@CanHazHugz ⇒ Future {
      addToClients(ClientSession(request))
    }
    case m: Tell ⇒ Future {
      registry.actorsFor(m.target).headOption foreach {
        _ ! m.payload
      }
    }
    case m: Ask ⇒ Future {
      registry.actorsFor(m.target).headOption map { r ⇒
        try {
          (r ? m.payload).as[ApplicationEvent] map {
            m respond _
          } getOrElse (m error "No reply")
        } catch {
          case e ⇒ m error e.getMessage
        }
      } getOrElse m.serviceUnavailable
    }
    case m: Listen ⇒ Future {
      remoteSubscriptions !(m, ClientSession(request))
    }
    case m: Deafen ⇒ Future {
      remoteSubscriptions !(m, ClientSession(request))
    }
    case m: Shout ⇒ Future {
      Vector(remoteSubscriptions, localSubscriptions) foreach {
        _ ! m
      }
    }
  }

  val serializer = new BorgZMQMessageSerializer

  private def deserialize(msg: ZMQMessage) = Messages(serializer.fromZMQMessage(msg))

  private def serialize(msg: BorgMessageWrapper) = Frame(msg.toBytes)

  private def mkReply(request: ZMQMessage, response: BorgMessageWrapper) = {
    Send(request.frames.dropRight(1) :+ serialize(response))
  }

  private def newRemoteSubscriptions = {
    registry.actorFor[Subscriptions.RemoteSubscriptions] getOrElse {
      val subs = actorOf[Subscriptions.RemoteSubscriptions]
      realSupervisor startLink subs
      subs
    }
  }

  private def newLocalSubscriptions = {
    registry.actorFor[Subscriptions.LocalSubscriptions] getOrElse {
      val subs = actorOf[Subscriptions.LocalSubscriptions]
      realSupervisor startLink subs
      subs
    }
  }

  private def realSupervisor: ActorRef = self.supervisor getOrElse self
}