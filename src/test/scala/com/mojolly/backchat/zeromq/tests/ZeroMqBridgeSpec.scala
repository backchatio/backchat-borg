package com.mojolly.backchat
package zeromq
package tests

import org.scalatest.matchers.MustMatchers
import org.zeromq.ZMQ
import akka.actor._
import org.multiverse.api.latches._
import java.util.concurrent.TimeUnit
import zeromq.Messages._
import Actor._
import net.liftweb.json._
import org.scalatest.{ BeforeAndAfterAll, WordSpec }

object ZeroMqBridgeSpec {
  val context = ZMQ.context(1)
}
class ZeroMqBridgeSpec extends WordSpec with MustMatchers with BeforeAndAfterAll {

  import ZeroMqBridgeSpec.context

  "A ZeroMQ bridge actor" should {

    "forward the application event for a fire and forget message" in {
      val name = "the-actor-fireforget-test"
      val latch = new StandardLatch
      val dealer = context.socket(Dealer)
      dealer.bind("inproc://" + name + ".inproc")
      val serverActor = actorOf(new Actor {
        self.id = "the-topic"
        protected def receive = {
          case ApplicationEvent('published_command, JNothing) ⇒ {
            latch.open()
          }
        }
      }).start()
      val bridgeActor = actorOf(new ZeroMqBridge(context, name) with ServerBridge).start()
      bridgeActor ! ProtocolMessage(Enqueue("the-topic", ApplicationEvent('published_command)).toZMessage)
      latch.tryAwait(2, TimeUnit.SECONDS) must be(true)
      serverActor.stop()
    }

    "forward the application event for a pubsub notification" in {
      val name = "the-actor-pubsubnotify-test"
      val latch = new StandardLatch
      val dealer = context.socket(Dealer)
      dealer.bind("inproc://" + name + ".inproc")
      val bridgeActor = actorOf(new ZeroMqBridge(context, name) with ServerBridge with SubscriberBridge).start()
      val subscribeLatch = new StandardLatch
      val serverActor = actorOf(new Actor {
        self.id = "the-topic"
        override def preStart {
          bridgeActor ! Subscribe(self.id)
          subscribeLatch.open()
        }
        protected def receive = {
          case ApplicationEvent('notify, _) ⇒ {
            latch.open()
          }
        }
      }).start()
      subscribeLatch.tryAwait(2, TimeUnit.SECONDS) must be(true)
      bridgeActor ! ProtocolMessage(new Uuid(newCcId), "pubsub", Some("publish"), "the-topic", ApplicationEvent('notify))
      latch.tryAwait(2, TimeUnit.SECONDS) must be(true)
      serverActor.stop()
    }

    "dispatch the application event for a request" in {
      val name = "the-actor-reply-test"
      val latch = new StandardLatch
      val dealer = context.socket(Dealer)
      val poller = new ZeroMQ.ZeroMQPoller(context)
      val ccid = newCcId
      val serverActor = actorOf(new Actor {
        self.id = "the-req-target"
        protected def receive = {
          case ApplicationEvent('the_request, _) ⇒ {
            self.sender foreach { _ ! JsonParser.parse(ApplicationEvent('the_reply).toJson) }
          }
        }
      }).start()
      poller.init()
      poller += (dealer -> ((msg: ZMessage) ⇒ {
        ProtocolMessage(msg) match {
          case ProtocolMessage(cc, "requestreply", None, "the-client", "[\"the_reply\"]") if cc.toString == ccid ⇒ {
            latch.open()
          }
          case x ⇒
        }
      }))
      dealer.bind("inproc://" + name + ".inproc")
      val bridgeActor = actorOf(new ZeroMqBridge(context, name) with ServerBridge).start()
      bridgeActor ! ProtocolMessage(Request("the-req-target", ApplicationEvent('the_request), new Uuid(ccid), "the-client").toZMessage.wrap("another-socket"))
      poller.poll(2000) // get ready message out
      poller.poll(2000) // get the actual publish message
      latch.tryAwait(10, TimeUnit.MILLISECONDS) must be(true)
      poller -= dealer
      dealer.close()
      serverActor.stop()
    }

    "publish the application event to the topic" in {
      val name = "the-actor-pubsub-test"
      val latch = new StandardLatch
      val dealer = context.socket(Dealer)
      val poller = new ZeroMQ.ZeroMQPoller(context)
      poller.init()
      poller += (dealer -> ((msg: ZMessage) ⇒ {
        ProtocolMessage(msg) match {
          case ProtocolMessage(_, "pubsub", Some("publish"), "the-topic", "[\"notify\"]") ⇒ latch.open()
          case x ⇒
        }
      }))
      dealer.bind("inproc://" + name + ".inproc")
      val bridgeActor = actorOf(new ZeroMqBridge(context, name) with ServerBridge with PublisherBridge).start()
      bridgeActor ! TopicSubscription("the-topic", Subscription(Seq.empty))
      bridgeActor ! Publish("the-topic", ApplicationEvent('notify))
      poller.poll(2000) // get ready message out
      poller.poll(2000) // get the actual publish message
      latch.tryAwait(10, TimeUnit.MILLISECONDS) must be(true)
      poller -= dealer
      dealer.close()
    }

  }

}