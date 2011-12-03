package backchat
package borg
package telepathy

import akka.actor._
import akka.zeromq._
import akka.dispatch.CompletableFuture
import telepathy.Messages.{ Reply, Ask, Tell }

case class TelepathClientConfig(server: TelepathAddress, listener: Option[ActorRef] = None)

object Client {

}

class Client(config: TelepathClientConfig) extends Telepath {

  lazy val socket = newSocket(SocketType.Dealer, Linger(0L))
  var activeRequests = Map.empty[Uuid, CompletableFuture[Any]]

  override def preStart() {
    self ! 'init
  }

  protected def receive = {
    case 'init ⇒ {
      socket ! Connect(config.server.address)
      logger trace "Establishing connection to server"
    }
    case m: Tell ⇒ {
      logger trace "enqueuing a message to a server: %s".format(m)
      socket ! serialize(m)
    }
    case m: Ask ⇒ {
      logger trace "Sending a request to a server: %s".format(m)
      self.senderFuture foreach { fut ⇒
        activeRequests += m.ccid -> fut
        socket ! serialize(m)
      }
    }
    case m: ZMQMessage ⇒ deserialize(m) match {
      case rep: Reply ⇒ {
        logger trace "processing a reply: %s".format(rep)
        activeRequests get rep.ccid foreach { _ completeWithResult rep.payload }
      }
    }
    case Connecting ⇒ {
      logger info "Connected to server"
      config.listener foreach { _ ! 'Connected }
    }
    case Closed ⇒ {
      logger debug "Socket connection closed"
    }
  }

  val serializer = new BorgZMQMessageSerializer
  private def deserialize(msg: ZMQMessage) = Messages(serializer.fromZMQMessage(msg))
  private def serialize(msg: BorgMessageWrapper) = serializer.toZMQMessage(msg.unwrapped)
}