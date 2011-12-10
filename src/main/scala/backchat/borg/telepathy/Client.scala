package backchat
package borg
package telepathy

import akka.actor._
import Actor._
import akka.zeromq._
import akka.dispatch.CompletableFuture
import telepathy.Messages._
import telepathy.Subscriptions.Do

case class TelepathClientConfig(server: TelepathAddress, listener: Option[ActorRef] = None, subscriptionManager: Option[ActorRef] = None)

class Client(config: TelepathClientConfig) extends Telepath {

  lazy val socket = newSocket(SocketType.Dealer, Linger(0L))
  var activeRequests = Map.empty[Uuid, CompletableFuture[Any]]

  val subscriptionManager = config.subscriptionManager getOrElse spawnSubscriptionManager

  self.id = config.server.address

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
    case m: Shout ⇒ {
      logger trace "Sending publish to a server: %s".format(m)
      socket ! serialize(m)
    }
    case m: Listen ⇒ {
      logger trace "Sending subscribe to a server: %s".format(m)
      subscriptionManager ! (m, self.sender.get)
    }
    case m: Deafen ⇒ {
      logger trace "Sending unsubscribe to a server: %s".format(m)
      subscriptionManager ! (m, self.sender.get)
    }
    case Do(req) ⇒ socket ! serialize(req) // subscription manager callbacks
    case m: ZMQMessage ⇒ deserialize(m) match { // incoming message handler
      case rep: Reply ⇒ {
        logger trace "processing a reply: %s".format(rep)
        activeRequests get rep.ccid foreach { _ completeWithResult rep.payload }
      }
      case shout: Shout ⇒ {
        logger trace "processing publish to local subscriptions: %s".format(shout)
        subscriptionManager ! shout
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

  private def spawnSubscriptionManager = {
    registry.actorFor[Subscriptions.LocalSubscriptions] getOrElse {
      val subs = actorOf[Subscriptions.LocalSubscriptions]
      realSupervisor startLink subs
      subs
    }
  }

  private def realSupervisor: ActorRef = self.supervisor getOrElse self
}