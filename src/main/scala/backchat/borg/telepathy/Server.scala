package backchat
package borg
package telepathy

import Messages._
import akka.actor._
import Actor._
import akka.zeromq._
import akka.dispatch.{ Future }

case class ServerConfig(listenOn: TelepathAddress, socket: Option[ActorRef] = None)
object ClientSession {
  def apply(request: ZMQMessage): ClientSession = ClientSession(request.frames.head.payload)
}
case class ClientSession(clientId: Seq[Byte])
object Server {
  type Respond = PartialFunction[BorgMessageWrapper, Future[Any]]
}
class Server(config: ServerConfig) extends Telepath {

  import Server._
  lazy val socket = config.socket getOrElse newSocket(SocketType.Router, Linger(0L))
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
    case request: ZMQMessage if responseFor.isDefinedAt(deserialize(request)) ⇒
      (deserialize _ andThen hugClientIfInNeed(request) andThen responseFor)(request) onResult {
        case response: BorgMessageWrapper ⇒ socket ! mkReply(request, response)
        case 'addToClients                ⇒ addToClients(ClientSession(request))
        case _                            ⇒ // ignore the other ones 
      }
  }

  protected def addToClients(session: ClientSession) = {
    if (!(activeClients contains session))
      activeClients :+= session
  }

  def hugClientIfInNeed(request: ZMQMessage): PartialFunction[BorgMessageWrapper, BorgMessageWrapper] = {
    case m: HiveRequest ⇒ {
      if (activeClients contains ClientSession(request)) socket ! Hug(m.ccid)
      m
    }
    case m ⇒ m
  }

  protected val responseFor: Respond = {
    case Ping           ⇒ Future { Pong }
    case a @ CanHazHugz ⇒ Future { 'addToClients }
    case m: Tell        ⇒ Future { registry.actorsFor(m.target).headOption foreach { _ ! m.payload } }
  }

  val serializer = new BorgZMQMessageSerializer
  private def deserialize(msg: ZMQMessage) = Messages(serializer.fromZMQMessage(msg))
  private def serialize(msg: BorgMessageWrapper) = Frame(msg.toBytes)
  private def mkReply(request: ZMQMessage, response: BorgMessageWrapper) = {
    Send(request.frames.dropRight(1) :+ serialize(response))
  }
}