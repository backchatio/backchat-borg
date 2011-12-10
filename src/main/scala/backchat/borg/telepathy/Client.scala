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
    self ! Init
  }

  protected def receive = happyGoLucky
  
  protected def manageLifeCycle: Receive = {
    case Init ⇒ {
      socket ! Connect(config.server.address)
      logger trace "Establishing connection to server"
    }
    case Paranoid => {
      become(paranoid)
      // TODO: start pinging
    }
    case HappyGoLucky => {
      // TODO: check if pings are active and disable those
      become(happyGoLucky)
    }
    case Connecting ⇒ {
      logger info "Connected to server"
      config.listener foreach { _ ! 'Connected }
    }
    case Closed ⇒ {
      logger debug "Socket connection closed"
    }    
  }
  
  protected def paranoid: Receive = manageLifeCycle orElse { // TODO: Actually provide the reliabillity
    case m: Tell ⇒ {
      logger trace "enqueuing a message to a server: %s".format(m)
      sendToSocket(m)
    }
    case m: Ask ⇒ {
      logger trace "Sending a request to a server: %s".format(m)
      self.senderFuture foreach { fut ⇒
        activeRequests += m.ccid -> fut
        sendToSocket(m)
      }
    }
    case m: Shout ⇒ {
      logger trace "Sending publish to a server: %s".format(m)
      sendToSocket(m)
    }
    case m: Listen ⇒ {
      logger trace "Sending subscribe to a server: %s".format(m)
      subscriptionManager ! (m, self.sender.get)
    }
    case m: Deafen ⇒ {
      logger trace "Sending unsubscribe to a server: %s".format(m)
      subscriptionManager ! (m, self.sender.get)
    }
    case Do(req) ⇒ sendToSocket(m) // subscription manager callbacks
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
  }
  
  protected def sendToSocket(m: HiveRequest) = {
    socket ! serialize(m)
  }

  protected def happyGoLucky: Receive = manageLifeCycle orElse {
    case m: Tell ⇒ {
      logger trace "enqueuing a message to a server: %s".format(m)
      sendToSocket(m)
    }
    case m: Ask ⇒ {
      logger trace "Sending a request to a server: %s".format(m)
      self.senderFuture foreach { fut ⇒
        activeRequests += m.ccid -> fut
        sendToSocket(m)
      }
    }
    case m: Shout ⇒ {
      logger trace "Sending publish to a server: %s".format(m)
      sendToSocket(m)
    }
    case m: Listen ⇒ {
      logger trace "Sending subscribe to a server: %s".format(m)
      subscriptionManager ! (m, self.sender.get)
    }
    case m: Deafen ⇒ {
      logger trace "Sending unsubscribe to a server: %s".format(m)
      subscriptionManager ! (m, self.sender.get)
    }
    case Do(req) ⇒ sendToSocket(m) // subscription manager callbacks
    case m: ZMQMessage ⇒ deserialize(m) match { // incoming message handler
      case rep: Reply ⇒ {
        logger trace "processing a reply: %s".format(rep)
        activeRequests get rep.ccid foreach { _ completeWithResult rep.payload }
      }
      case shout: Shout ⇒ {
        logger trace "processing publish to local subscriptions: %s".format(shout)
        subscriptionManager ! shout
      }
      case Pong => //ignore
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