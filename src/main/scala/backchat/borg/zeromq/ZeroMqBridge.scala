package backchat
package borg
package zeromq

import akka.actor._
import akka.actor.Actor._
import akka.config.Supervision._
import Messages._
import net.liftweb.json.JsonAST.JValue
import java.util.concurrent.TimeUnit
import akka.dispatch.{ CompletableFuture }
import net.liftweb.json.Serialization
import com.weiglewilczek.slf4s.Logging
import org.scala_tools.time.Imports._

object ZeroMqBridge {

  def actorId(deviceName: String) = "backchat-∅MQ-" + deviceName + "-bridge"
  private[borg] case object ExitIfExpired
  private case object Dispatch

  private[borg] abstract class RequestDispatcher(bridge: ActorRef, protocolMessage: ProtocolMessage) extends Actor with Logging {
    self.lifeCycle = Temporary
    val created = DateTime.now

    override def preStart {
      self ! Dispatch
    }

    protected def receive = {
      case Dispatch ⇒ {
        logger debug "Executing dispatch"
        dispatchRequest()
      }
      case data: JValue ⇒ {
        bridge ! ('reply, protocolMessage.copy(payload = data.toJson, target = protocolMessage.sender.get, sender = None))
        self.stop()
      }
      case ExitIfExpired ⇒ {
        if (2.minutes.ago > created) {
          logger warn "Received NO reply for the protocol message: %s".format(protocolMessage)
          self.stop()
        }
      }
    }

    protected def dispatchRequest()

  }
  private[borg] class DefaultRequestDispatcher(bridge: ActorRef, protocolMessage: ProtocolMessage) extends RequestDispatcher(bridge, protocolMessage) {
    protected def dispatchRequest() {
      registry.actorsFor(protocolMessage.target).headOption foreach { _ ! ApplicationEvent(protocolMessage.payload) }
    }
  }
  private[borg] class ServiceRegistryRequestDispatcher(bridge: ActorRef, protocolMessage: ProtocolMessage) extends RequestDispatcher(bridge, protocolMessage) {
    protected def dispatchRequest() {
      registry.actorFor[ServiceRegistry] foreach { _ ! Request(protocolMessage.target, ApplicationEvent(protocolMessage.payload)) }
    }
  }

  private[borg] class ReplyClient(ccid: String, reqFuture: CompletableFuture[Any])(implicit recvTimeout: Actor.Timeout) extends Actor with Logging {
    self.id = "reply-" + ccid
    self.lifeCycle = Temporary

    protected def receive = {
      case m: ApplicationEvent ⇒ {
        reqFuture.completeWithResult(m)
        self.stop()
      }
      case "SERVER_UNAVAILABLE" ⇒ {
        reqFuture.completeWithException(new ServerUnavailableException)
        self.stop()
      }
      case "TIMEOUT" ⇒ {
        reqFuture.completeWithException(new RequestTimeoutException("Request timed out"))
        self.stop()
      }
    }

    override def postStop {
      logger debug ("Stoppped the reply actor for %s" format ccid)
    }
  }

  private[borg] class RemoteSubscriptionManager(publisher: ActorRef) extends Actor with Logging {
    self.id = "zeromq-remote-subscription-manager"

    private var topicSubscriptions = Map[String, Set[Subscription]]()
    private var globalSubscriptions = Set[Subscription]()

    private def subscribe(topic: String, subscriber: Subscription) {
      logger debug ("Subscribing to: %s" format topic)
      if (topic.isBlank) {
        globalSubscriptions += subscriber
      } else {
        if (topicSubscriptions.contains(topic)) {
          topicSubscriptions += topic -> (topicSubscriptions(topic) + subscriber)
        } else {
          topicSubscriptions += topic -> Set(subscriber)
        }
      }
      logger debug "Global subs: %s\nTopic subs: %s".format(globalSubscriptions, topicSubscriptions)
    }

    private def unsubscribe(topic: String, subscriber: Subscription) {
      logger debug ("Unsubscribing from: %s" format topic)
      if (topic.isBlank) {
        globalSubscriptions -= subscriber
      } else {
        if (topicSubscriptions.contains(topic)) {
          val subs = topicSubscriptions(topic)
          if (subs.size > 1) {
            if (subs.contains(subscriber)) {
              topicSubscriptions += topic -> (subs - subscriber)
            }
          } else {
            topicSubscriptions -= topic
          }
        }
      }
    }

    protected def receive = {
      case TopicSubscription(topic, subscriber) ⇒ {
        subscribe(topic, subscriber)
      }
      case (SubscribeAll, subscriber: Subscription) ⇒ {
        globalSubscriptions += subscriber
      }
      case TopicUnsubscription(topic, subscriber) ⇒ {
        unsubscribe(topic, subscriber)
      }
      case (UnsubscribeAll, subscriber: Subscription) ⇒ {
        globalSubscriptions -= subscriber
      }
      case Publish(topic, payload) ⇒ {
        logger debug "Got publish request to: %s with %s".format(topic, payload.toJson)
        val ccid = newCcId
        (globalSubscriptions ++ topicSubscriptions.filterKeys(topic.startsWith(_)).flatMap(_._2).toSet) foreach {
          publisher ! PublishTo(_, topic, payload)
        }
      }
    }
  }

  private[borg] class SubscriptionManager extends Actor with Logging {

    self.id = "zeromq-subscription-manager"

    private var topicSubscriptions = Map[String, Set[ActorRef]]()
    private var globalSubscriptions = Set[ActorRef]()

    private def subscribe(topic: String, subscriber: ActorRef) {
      logger debug ("Subscribing to: %s" format topic)
      if (topic.isBlank) {
        globalSubscriptions += subscriber
      } else {
        if (topicSubscriptions.contains(topic)) {
          topicSubscriptions += topic -> (topicSubscriptions(topic) + subscriber)
        } else {
          topicSubscriptions += topic -> Set(subscriber)
        }
      }
    }

    private def unsubscribe(topic: String, subscriber: ActorRef) {
      logger debug ("Unsubscribing from: %s" format topic)
      if (topic.isBlank) {
        globalSubscriptions -= subscriber
      } else {
        if (topicSubscriptions.contains(topic)) {
          val subs = topicSubscriptions(topic)
          if (subs.size > 1) {
            if (subs.contains(subscriber)) {
              topicSubscriptions += topic -> (subs - subscriber)
            }
          } else {
            topicSubscriptions -= topic
          }
        }
      }
    }

    protected def receive = {
      case (Subscribe(topic), subscriber: ActorRef) ⇒ {
        if (globalSubscriptions.isEmpty && !topicSubscriptions.contains(topic)) self.sender foreach { _ ! Do(Subscribe(topic)) }
        subscribe(topic, subscriber)
      }
      case (SubscribeAll, subscriber: ActorRef) ⇒ {
        if (globalSubscriptions.isEmpty) self.sender foreach { _ ! Do(SubscribeAll) }
        globalSubscriptions += subscriber
      }
      case (Unsubscribe(topic), subscriber: ActorRef) ⇒ {
        unsubscribe(topic, subscriber)
        val subs = topicSubscriptions.get(topic)
        if (subs.isEmpty || subs.forall(_.isEmpty)) self.sender foreach { _ ! Do(Unsubscribe(topic)) }
      }
      case (UnsubscribeAll, subscriber: ActorRef) ⇒ {
        globalSubscriptions -= subscriber
        if (globalSubscriptions.isEmpty && (topicSubscriptions.isEmpty || topicSubscriptions.values.forall(_.isEmpty))) {
          self.sender foreach { _ ! Do(UnsubscribeAll) }
        }
      }
      case ProtocolMessage(_, "pubsub", Some("publish"), topic, payload) ⇒ {
        (globalSubscriptions ++ topicSubscriptions.filterKeys(topic.startsWith(_)).flatMap(_._2).toSet) foreach {
          _ ! ApplicationEvent(payload)
        }
      }
    }
  }

}
case class Do(m: RequestMessage)
trait SubscriberBridge { parent: ZeroMqBridge ⇒

  become(subscribeToEvents orElse parent.receive, false)
  lazy val subscriptionManager = self.spawnLink[ZeroMqBridge.SubscriptionManager]

  protected def subscribeToEvents: Receive = {
    case m: Subscribe ⇒ {
      logger debug "got subscribe request: %s for %s".format(m, zeroMqBridge)
      subscriptionManager ! (m, self.sender.get)
    }
    case SubscribeAll ⇒ {
      subscriptionManager ! (SubscribeAll, self.sender.get)
    }
    case m: Unsubscribe ⇒ {
      subscriptionManager ! (m, self.sender.get)
    }
    case UnsubscribeAll ⇒ {
      subscriptionManager ! (UnsubscribeAll, self.sender.get)
    }
    case Do(msg) ⇒ {
      logger trace ("Got Do: %s" format msg)
      sendToBridge(msg.toZMessage)
    }
    case n@ProtocolMessage(_, "pubsub", Some("publish"), _, _) ⇒ {
      subscriptionManager ! n
    }
  }

}
sealed trait RemoteSubscriptionManagement
case class Subscription(addresses: Seq[Array[Byte]]) extends RemoteSubscriptionManagement
case class TopicSubscription(topic: String, subscription: Subscription) extends RemoteSubscriptionManagement
case class TopicUnsubscription(topic: String, subscription: Subscription) extends RemoteSubscriptionManagement
case class PublishTo(subscription: Subscription, topic: String, payload: ApplicationEvent)

trait PublisherBridge { self: ZeroMqBridge ⇒

  protected val subscriptions = {
    val a = actorOf(new ZeroMqBridge.RemoteSubscriptionManager(self))
    self.startLink(a)
    a
  }

  become(publishMessage orElse self.receive, false)
  protected def publishMessage: Receive = {
    case m: RemoteSubscriptionManagement ⇒ {
      logger trace ("managing remote subscription: %s" format m)
      subscriptions ! m
    }
    case m: Publish ⇒ subscriptions ! m
    case PublishTo(Subscription(addresses), topic, event) ⇒ {
      logger.trace("publishing to '%s': %s".format(topic, event))
      val zmsg = ZMessage("", newCcId, "pubsub", "publish", topic, event.toJson)
      zmsg.addresses = addresses
      sendToBridge(zmsg)
    }
  }
}

trait ZmqBridge { parent: ZeroMqBridge ⇒
  protected def bridgeMessage: Receive
}
class RequestRequiresFutureException extends Exception("To make a request we require a future '!!' or '!!!'.")
trait ClientBridge { parent: ZeroMqBridge ⇒


  protected def bridgeMessage: Receive = {
    case m@ProtocolMessage(_, "requestreply", None, target, payload) ⇒ { // when sender is missing it's a reply
      logger trace ("received a reply: %s" format m)
      registry.actorsFor(target).headOption foreach { _ ! ApplicationEvent(payload) }
    }
    case m@ProtocolMessage(ccid, "system", Some("ERROR"), _, message) ⇒ {
      logger trace ("received an error: %s" format m)
      val replyActors = registry.actorsFor("reply-" + ccid)
      replyActors foreach { _ ! message }
      if (replyActors.isEmpty) {
        logger error ("Protocol error: %s" format message)
        throw new ServerUnavailableException
      }
    }
    case m: Request ⇒ {
      if (!self.senderFuture().isDefined) throw new RequestRequiresFutureException
      implicit val timeout = Actor.Timeout(akka.util.Duration(self.senderFuture().get.timeoutInNanos, TimeUnit.NANOSECONDS))
      val requester = actorOf(new ZeroMqBridge.ReplyClient(m.ccid.toString, self.senderFuture().get)).start()
      sendToBridge(m.copy(sender = requester.id).toZMessage)
    }
    case m: Enqueue ⇒ sendToBridge(m.toZMessage)
  }
}

//trait ServiceRegistration { parent: ZeroMqBridge =>
//  protected def registerService = {
//    case m: RegisterService => {
//      sendToBridge(m.toZMessage)
//    }
//    case m: UnregisterService => {
//      sendToBridge(m.toZMessage)
//    }
//  }
//}

trait ServerBridge { parent: ZeroMqBridge ⇒

  Scheduler.schedule(() ⇒ {
    registry.actorsFor[ZeroMqBridge.RequestDispatcher] foreach { _ ! ZeroMqBridge.ExitIfExpired }
  }, 2, 2, TimeUnit.MINUTES)

  protected def bridgeMessage: Receive = {
    case m@ProtocolMessage(_, "requestreply", _, _, _) ⇒ { // when sender is present it's a request
      self startLink actorOf(new ZeroMqBridge.DefaultRequestDispatcher(self, m))
    }
    case m@ProtocolMessage(_, "fireforget", _, target, payload) ⇒ {
      registry.actorsFor(target).headOption foreach { _ ! ApplicationEvent(payload) }
    }
    case ('reply, m: ProtocolMessage) ⇒ {
      sendToBridge(m.toZMessage)
    }
  }
}

trait ServiceRegistryBridge { parent: ZeroMqBridge with ServerBridge ⇒

  become(routeThroughRegistry orElse parent.receive, false)

  protected def routeThroughRegistry: Receive = {
    case m@ProtocolMessage(_, "system", Some("HELLO"), _, _) ⇒ {
      registry.actorFor[ServiceRegistry] foreach { _ ! ServiceListRequest(m) }
    }
    case m@ProtocolMessage(_, "requestreply", _, _, _) ⇒ { // when sender is present it's a request
      self startLink actorOf(new ZeroMqBridge.ServiceRegistryRequestDispatcher(self, m))
    }
    case m@ProtocolMessage(_, "fireforget", _, target, payload) ⇒ {
      registry.actorFor[ServiceRegistry] foreach { _ ! Enqueue(target, ApplicationEvent(payload)) }
    }
    case ServiceList(services, m) ⇒ sendToBridge {
      val msg = m.toZMessage
      msg.sender = "CAPABILITIES"
      msg.target = "1.0"
      msg.body = Serialization.write(services)
      msg
    }
  }
}
abstract class ZeroMqBridge(context: Context, deviceName: String) extends Actor with ZmqBridge with Logging {

  ZeroMQ.bridgeDispatcher(self)
  self.id = ZeroMqBridge.actorId(deviceName)

  protected var zeroMqBridge: Option[Socket] = None

  protected def receive = bridgeMessage orElse {
    case m: ProtocolMessage ⇒ {
      logger warn ("unhandled protocol message: %s" format m)
    }
  }

  protected def sendToBridge(msg: ZMessage) {
    zeroMqBridge foreach { msg(_) }
  }

  override def preRestart(reason: Throwable) {
    logger.warn("There was a problem in %s" format self.id, reason)
    zeroMqBridge foreach { _.close() }
    zeroMqBridge = None
  }

  override def postStop {
    val i = self.linkedActors.values.iterator
    while (i.hasNext) {
      val ref = i.next
      ref.stop()
      self.unlink(ref)
    }
    logger info ("Stopped %s" format self.id)
    preRestart(null)
  }

  override def preStart {
    logger info ("Starting %s" format self.id)
    logger trace ("pinned actor to dispatcher")
    val br = context.socket(Dealer)
    br.setIdentity(self.uuid.toString.getBytes(ZMessage.defaultCharset))
    br.connect("inproc://" + deviceName + ".inproc")
    br.setLinger(0L)
    logger trace ("created socket")
    Ready.toZMessage.wrap(self.uuid.toString, "")(br)
    logger trace ("Sent ready message to bridge")
    zeroMqBridge = Some(br)
  }
}

