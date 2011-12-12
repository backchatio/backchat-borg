package backchat
package borg
package telepathy

import Messages._
import akka.actor.ActorRef
import akka.zeromq._
import akka.dispatch.{ Future }

case class ServerConfig(listenOn: TelepathAddress, socket: Option[ActorRef] = None)
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
      (deserialize _ andThen responseFor)(request) onResult {
        case response: BorgMessageWrapper ⇒ socket ! mkReply(request, response)
        case 'addToClients                ⇒ activeClients :+= ClientSession(request.frames.head.payload)
        case _                            ⇒ // ignore the other ones 
      }
  }

  protected val responseFor: Respond = {
    case Ping           ⇒ Future { Pong }
    case a @ CanHazHugz ⇒ Future { 'addToClients }
  }

  val serializer = new BorgZMQMessageSerializer
  private def deserialize(msg: ZMQMessage) = Messages(serializer.fromZMQMessage(msg))
  private def serialize(msg: BorgMessageWrapper) = Frame(msg.toBytes)
  private def mkReply(request: ZMQMessage, response: BorgMessageWrapper) = {
    Send(request.frames.dropRight(1) :+ serialize(response))
  }
}