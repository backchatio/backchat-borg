package backchat
package borg
package telepathy

import akka.actor._
import Actor._
import akka.zeromq._
import telepathy.Messages._
import telepathy.Subscriptions.Do
import akka.dispatch.{ DefaultCompletableFuture, ActorCompletableFuture, Future, CompletableFuture }

case class TelepathClientConfig(
  server: TelepathAddress,
  retries: Int = 3,
  hugTimeout: Duration = 500.millis,
  listener: Option[ActorRef] = None,
  subscriptionManager: Option[ActorRef] = None)

case class ExpectedHug(
    sender: UntypedChannel,
    future: DefaultCompletableFuture[Hugging],
    count: Int = 0) {

  def incrementCount(newFuture: DefaultCompletableFuture[Hugging]) =
    copy(count = count + 1, future = newFuture)

  def gotIt = {
    future.completeWithResult(Hugged)
  }

}

class Client(config: TelepathClientConfig) extends Telepath {

  lazy val socket = newSocket(SocketType.Dealer, Linger(0L))
  var activeRequests = Map.empty[Uuid, CompletableFuture[Any]]
  var expectedHugs = Map.empty[Uuid, ExpectedHug]

  val subscriptionManager = config.subscriptionManager getOrElse spawnSubscriptionManager

  self.id = config.server.address

  override def preStart() {
    self ! Init
  }

  protected def receive = brainDead orElse happyGoLucky

  protected def brainDead: Receive = {
    case Init ⇒ {
      socket ! Connect(config.server.address)
      logger trace "Establishing connection to server"
    }
    case Paranoid ⇒ {
      become(brainDead orElse paranoid)
      sendToSocket(CanHazHugz)
      // TODO: start pinging
    }
    case HappyGoLucky ⇒ {
      // TODO: check if pings are active and disable those
      become(brainDead orElse happyGoLucky)
    }
    case Connecting ⇒ {
      logger info "Connected to server"
      config.listener foreach { _ ! 'Connected }
    }
    case Closed ⇒ {
      logger debug "Socket connection closed"
    }
  }

  protected def paranoid: Receive = {
    case m: Tell ⇒ {
      logger trace "enqueuing a message to a server: %s".format(m)
      sendToSocketAndExpectHug(m)
    }
    case m: Ask ⇒ {
      logger trace "Sending a request to a server: %s".format(m)
      println("Sending a request to a server: %s".format(m))
      self.senderFuture foreach { fut ⇒
        activeRequests += m.ccid -> fut
        sendToSocketAndExpectHug(m)
      }
    }
    case m: Shout ⇒ {
      logger trace "Sending publish to a server: %s".format(m)
      sendToSocketAndExpectHug(m)
    }
    case m: Listen ⇒ {
      logger trace "Sending subscribe to a server: %s".format(m)
      handleSubscription(m)
    }
    case m: Deafen ⇒ {
      logger trace "Sending unsubscribe to a server: %s".format(m)
      handleSubscription(m)
    }
    case Do(req) ⇒ // subscription manager callbacks
    case m: ZMQMessage ⇒ deserialize(m) match { // incoming message handler
      case rep: Reply ⇒ {
        logger trace "processing a reply: %s".format(rep)
        activeRequests get rep.ccid foreach { _ completeWithResult rep.payload }
      }
      case shout: Shout ⇒ {
        logger trace "processing publish to local subscriptions: %s".format(shout)
        subscriptionManager ! shout
      }
      case Hug(ccid) ⇒ {
        println("received a hug %s" format Hug(ccid))
        expectedHugs(ccid).gotIt
        expectedHugs -= ccid
      }
      case Pong ⇒ { //TODO: Handle Pong

      }
    }
  }

  protected def handleSubscription(m: HiveRequest) {
    val ch = self.channel
    sendToSocketAndExpectHug(m, { _ ⇒
      m match {
        case _: Listen | _: Deafen ⇒ subscriptionManager ! (m, ch)
        case _                     ⇒
      }
    })
  }

  protected def sendToSocketAndExpectHug(m: HiveRequest, onComplete: Hugging ⇒ Unit = defaultOnComplete _) {
    val fut = hugFuture(m, onComplete)
    expectedHugs += m.ccid -> {
      ExpectedHug(self.channel, fut)
    }
    sendToSocket(m)
  }

  protected def defaultOnComplete(res: Hugging) {}

  protected def hugFuture(m: HiveRequest, onComplete: Hugging ⇒ Unit): DefaultCompletableFuture[Hugging] =
    Future.empty[Hugging](config.hugTimeout.millis) onTimeout { _ ⇒ rescheduleHug(m, onComplete) } onResult {
      case Hugged ⇒ {
        onComplete(Hugged)
        expectedHugs -= m.ccid
      }
      case nl @ NoLovin(_) ⇒ {
        logger error ("Got no lovin' from the server, sad panda")
        expectedHugs.get(m.ccid) foreach { hug ⇒
          hug.sender match {
            case f: ActorCompletableFuture ⇒ f.completeWithResult(nl) //f.complete(Right(NoLovin(m)))
            case f: ActorRef               ⇒ f ! nl
            case _                         ⇒
          }
        }
        expectedHugs -= m.ccid
      }
    }

  protected def rescheduleHug(m: HiveRequest, onComplete: Hugging ⇒ Unit) = {
    expectedHugs.get(m.ccid) foreach { hug ⇒
      if (hug.count < config.retries) {
        logger debug ("retrying request: %s" format m)
        expectedHugs += m.ccid -> hug.incrementCount(hugFuture(m, onComplete))
        sendToSocket(m)
      } else {
        logger warn ("Got no lovin' from the server, sad panda")
        val nl = NoLovin(m)
        hug.sender match {
          case f: ActorCompletableFuture ⇒ f.completeWithResult(nl)
          case f: ActorRef               ⇒ f ! nl
          case _                         ⇒
        }
        expectedHugs -= m.ccid
      }
    }
  }

  protected def sendToSocket(m: BorgMessageWrapper) = {
    socket ! serialize(m)
  }

  protected def happyGoLucky: Receive = {
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
    case Do(req) ⇒ sendToSocket(req) // subscription manager callbacks
    case m: ZMQMessage ⇒ deserialize(m) match { // incoming message handler
      case rep: Reply ⇒ {
        logger trace "processing a reply: %s".format(rep)
        activeRequests get rep.ccid foreach { _ completeWithResult rep.payload }
      }
      case shout: Shout ⇒ {
        logger trace "processing publish to local subscriptions: %s".format(shout)
        subscriptionManager ! shout
      }
      case Pong | _: Hug ⇒ //ignore
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