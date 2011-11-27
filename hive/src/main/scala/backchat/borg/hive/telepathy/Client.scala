package backchat
package borg
package hive
package telepathy

import akka.actor._
import akka.zeromq._
import collection.mutable
import akka.dispatch.{CompletableFuture, Future}
import telepathy.Messages.{Reply, Ask, Tell}

case class TelepathClientConfig(server: TelepathAddress, listener: Option[ActorRef] = None)

class Client(config: TelepathClientConfig) extends Telepath {

  lazy val socket = newSocket(SocketType.Dealer)
  var activeRequests = Map.empty[Uuid, CompletableFuture[Any]]

  override def preStart() {
    self ! 'init
  }

  protected def receive = {
    case 'init => {
      socket ! Connect(config.server.address)
      logger debug "Establishing connection to server"
    }
    case m: Tell => {
      logger debug "enqueuing a message to a server"
      socket ! serialize(m)
    }
    case m: Ask => {
      logger debug "Sending a request to a server"
      self.senderFuture foreach { fut =>
        activeRequests += m.ccid -> fut
        socket ! serialize(m)
      }
    }
    case m: ZMQMessage => deserialize(m) match {
      case rep: Reply => {
        logger debug "processing a reply: %s".format(rep)
        activeRequests get rep.ccid foreach { fut =>
          logger debug "completing future"
          fut completeWithResult rep.payload
        }
      }
    }
    case Connecting => {
      logger debug "Connecting to server"
      config.listener foreach { _ ! 'Connected }
    }
    case Closed => {
      logger debug "Socket connection closed"
    }
  }
  
  val serializer = new BorgZMQMessageSerializer
  private def deserialize(msg: ZMQMessage) = Messages(serializer.fromZMQMessage(msg))
  private def serialize(msg: BorgMessageWrapper) = serializer.toZMQMessage(msg.unwrapped)
}