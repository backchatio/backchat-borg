package backchat
package borg
package telepathy

import akka.actor._
import akka.zeromq.Frame
import telepathy.Messages._

object Subscriptions {

  case class Do(msg: HiveRequest)
  sealed trait RemoteSubscriptionManagement
  case class Subscription(addresses: Seq[Frame]) extends RemoteSubscriptionManagement
  case class PublishTo(subscription: ClientSession, topic: String, payload: ApplicationEvent)

  class RemoteSubscriptions extends Actor with Logging {
    self.id = "borg-remote-subscriptions"

    private[telepathy] var topicSubscriptions = Map[String, Set[ClientSession]]()
    private[telepathy] var globalSubscriptions = Set[ClientSession]()

    private def subscribe(topic: String, subscriber: ClientSession) {
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

    private def unsubscribe(topic: String, subscriber: ClientSession) {
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
      case (Listen("" | null, _), subscriber: ClientSession) ⇒ {
        globalSubscriptions += subscriber
      }
      case (Listen(topic, _), subscriber: ClientSession) ⇒ {
        subscribe(topic, subscriber)
      }
      case ExpireClient(client) ⇒ {
        globalSubscriptions -= client
        topicSubscriptions foreach {
          case (k, v) ⇒ {
            if (v contains client) topicSubscriptions += k -> v.filterNot(_ == client)
          }
        }
      }
      case ExpireClients(clients) ⇒ {
        globalSubscriptions --= clients
        topicSubscriptions foreach {
          case (k, v) ⇒ {
            if (v exists clients.contains) topicSubscriptions += k -> v.filterNot(clients.contains)
          }
        }
      }
      case (Deafen("" | null, _), subscriber: ClientSession) ⇒ {
        globalSubscriptions -= subscriber
      }
      case (Deafen(topic, _), subscriber: ClientSession) ⇒ {
        unsubscribe(topic, subscriber)
      }
      case Shout(topic, payload, _) ⇒ {
        logger debug "Got publish request to: %s with %s".format(topic, payload.toPrettyJson)
        (globalSubscriptions ++ topicSubscriptions.filterKeys(topic.startsWith(_)).flatMap(_._2).toSet) foreach { sub ⇒
          self.sender foreach { _ ! PublishTo(sub, topic, payload) }
        }
      }
    }
  }

  abstract class ChannelSubscriptions extends Actor with Logging {
    protected[telepathy] var topicSubscriptions = Map[String, Set[UntypedChannel]]()
    protected[telepathy] var globalSubscriptions = Set[UntypedChannel]()

    protected def subscribe(topic: String, subscriber: UntypedChannel) {
      logger debug ("Subscribing to: %s" format topic)
      if (topic.isBlank) {
        globalSubscriptions += subscriber
        topicSubscriptions foreach {
          case (k, v) ⇒ topicSubscriptions += k -> v.filterNot(_ == subscriber)
        }
      } else {
        if (topicSubscriptions.contains(topic)) {
          topicSubscriptions += topic -> (topicSubscriptions(topic) + subscriber)
        } else {
          topicSubscriptions += topic -> Set(subscriber)
        }
      }
    }

    protected def unsubscribe(topic: String, subscriber: UntypedChannel) {
      logger debug ("Unsubscribing from: %s" format topic)
      if (topic.isBlank) {
        globalSubscriptions -= subscriber
        topicSubscriptions foreach {
          case (k, v) ⇒ topicSubscriptions += k -> v.filterNot(_ == subscriber)
        }
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

  }

  class LocalSubscriptions extends ChannelSubscriptions {
    self.id = "borg-local-subscription-proxy"

    protected def receive = {
      case (m @ Listen("" | null, _), subscriber: UntypedChannel) ⇒ {
        globalSubscriptions += subscriber
      }
      case (m @ Listen(topic, _), subscriber: UntypedChannel) ⇒ {
        subscribe(topic, subscriber)
      }
      case (m @ Deafen("" | null, _), subscriber: UntypedChannel) ⇒ {
        globalSubscriptions -= subscriber
      }
      case (m @ Deafen(topic, _), subscriber: UntypedChannel) ⇒ {
        unsubscribe(topic, subscriber)
      }
      case Shout(topic, payload, _) ⇒ {
        val matches = globalSubscriptions ++ topicSubscriptions.filterKeys(topic.startsWith(_)).flatMap(_._2).toSet
        matches foreach { _ ! payload }
      }
    }

  }

  class RemoteSubscriptionPolicy extends ChannelSubscriptions {

    self.id = "borg-local-subscription-proxy"

    protected def receive = {
      case (m @ Listen("" | null, _), subscriber: UntypedChannel) ⇒ {
        if (globalSubscriptions.isEmpty) self.sender foreach { _.!(Do(m))(subscriber) }
        globalSubscriptions += subscriber
      }
      case (m @ Listen(topic, _), subscriber: UntypedChannel) ⇒ {
        if (globalSubscriptions.isEmpty && !topicSubscriptions.contains(topic)) self.sender foreach { _.!(Do(m))(subscriber) }
        subscribe(topic, subscriber)
      }
      case (m @ Deafen("" | null, _), subscriber: UntypedChannel) ⇒ {
        globalSubscriptions -= subscriber
        if (globalSubscriptions.isEmpty && (topicSubscriptions.isEmpty || topicSubscriptions.values.forall(_.isEmpty))) {
          self.sender foreach { _.!(Do(m))(subscriber) }
        }
      }
      case (m @ Deafen(topic, _), subscriber: UntypedChannel) ⇒ {
        unsubscribe(topic, subscriber)
        val subs = topicSubscriptions.get(topic)
        if (subs.isEmpty || subs.forall(_.isEmpty)) self.sender foreach { _.!(Do(m))(subscriber) }
      }
      case Shout(topic, payload, _) ⇒ {
        val matches = globalSubscriptions ++ topicSubscriptions.filterKeys(topic.startsWith(_)).flatMap(_._2).toSet
        matches foreach { _ ! payload }
      }
    }
  }

}