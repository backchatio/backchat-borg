package backchat
package zeromq
package tests

import org.scalatest.WordSpec
import org.scalatest.matchers.MustMatchers
import org.multiverse.api.latches.StandardLatch
import Messages._
import akka.actor._
import Actor._
import akka.config.Supervision._
import org.zeromq.ZMQ
import java.util.concurrent.{ CountDownLatch, TimeUnit }

object ZeroMqClientActorBridgeSpec {
  val context = ZMQ context 1
}
class ZeroMqClientActorBridgeSpec extends WordSpec with MustMatchers {

  import ZeroMqClientActorBridgeSpec._

  "A ZeroMqClientActorBridge" should {

    "be able to enqueue a message for the backend" in {
      val name = "zeromq-actor-client-test"
      val router = context.socket(Router)
      router.bind("inproc://" + name + ".inproc")
      val poller = new ZeroMQ.ZeroMQPoller(context)
      val latch = new StandardLatch
      poller += (router -> ((msg: ZMessage) ⇒ {
        if (msg.messageType == "fireforget" && msg.target == "the-target" && msg.body == ApplicationEvent('pingping).toJson) {
          latch.open()
        }
      }))
      poller.init()
      val client = actorOf(new ZeroMqBridge(context, name) with ClientBridge).start()
      client ! Enqueue("the-target", ApplicationEvent('pingping))
      poller.poll(2000)
      poller.poll(2000)
      latch.tryAwait(10, TimeUnit.MILLISECONDS) must be(true)
      client.stop()
      poller.dispose()
      router.close()
    }

    "be able to subscribe to events from the backend" in {
      val name = "zeromq-actor-client-subscribe-test"
      val router = context.socket(Router)
      router.bind("inproc://" + name + ".inproc")
      val poller = new ZeroMQ.ZeroMQPoller(context)
      val latch = new StandardLatch
      poller += (router -> ((msg: ZMessage) ⇒ {
        if (msg.messageType == "pubsub"
          && msg.sender == "subscribe"
          && msg.target == "the-target"
          && msg.body.isEmpty) {
          latch.open()
        }
      }))
      poller.init()
      val bridge = actorOf(new ZeroMqBridge(context, name) with ClientBridge with SubscriberBridge).start()
      val client = actorOf(new Actor {
        protected def receive = {
          case 'subscribe ⇒ bridge ! Subscribe("the-target")
        }
      }).start()
      client ! 'subscribe
      poller.poll(2000)
      poller.poll(2000)
      latch.tryAwait(10, TimeUnit.MILLISECONDS) must be(true)
      client.stop()
      bridge.stop()
      poller.dispose()
    }

    "be able to receive replies from the backend" in {
      val name = "zeromq-actor-client-reply-test"
      val router = context.socket(Router)
      router.bind("inproc://" + name + ".inproc")
      val poller = new ZeroMQ.ZeroMQPoller(context)
      val latch = new StandardLatch
      var id = ""
      poller += (router -> ((msg: ZMessage) ⇒ {
        if (msg.messageType == "requestreply" && msg.sender.isNotBlank && msg.target == "the-target" && msg.body == ApplicationEvent('pingping).toJson) {
          latch.open()
          id = msg.sender
        }
      }))
      poller.init()
      val bridge = actorOf(new ZeroMqBridge(context, name) with ClientBridge with SubscriberBridge).start()
      val replyLatch = new StandardLatch
      val client = actorOf(new Actor {
        self.id = name + "-actor"
        protected def receive = {
          case 'request ⇒ {
            val repl = (bridge ? Request("the-target", ApplicationEvent('pingping))).as[ApplicationEvent]
            if (repl.forall(_.action == 'pongpong)) replyLatch.open()
          }
        }
      }).start()
      client ! 'request
      poller.poll(2000)
      poller.poll(2000)
      latch.tryAwait(10, TimeUnit.MILLISECONDS) must be(true)
      bridge ! ProtocolMessage(new Uuid(), "requestreply", None, id, ApplicationEvent('pongpong))
      replyLatch.tryAwait(2, TimeUnit.SECONDS) must be(true)
      client.stop()
      bridge.stop()
      poller.dispose()
    }

    "be able to handle a SERVER_UNAVAILABLE message when sending READY" in {
      val name = "zeromq-actor-client-serverunavailable-test"
      val router = context.socket(Router)
      router.bind("inproc://" + name + ".inproc")
      val poller = new ZeroMQ.ZeroMQPoller(context)
      val latch = new StandardLatch
      var bridge: ActorRef = null
      poller += (router -> ((msg: ZMessage) ⇒ {
        msg.unwrap()

        val m = ZMessage("", msg.ccid, "system", "ERROR", "", "SERVER_UNAVAILABLE")
        m.addresses = msg.addresses
        bridge ! ProtocolMessage(m)
        latch.open()
      }))
      poller.init()
      val replyLatch = new StandardLatch
      bridge = actorOf(new ZeroMqBridge(context, name) with ClientBridge with SubscriberBridge {
        override protected def receive = {
          case m@ProtocolMessage(_, "system", Some("ERROR"), "", "SERVER_UNAVAILABLE") ⇒ {
            try {
              super.receive(m)
            } catch {
              case e: ServerUnavailableException ⇒ {
                replyLatch.open()
                throw e
              }
            }
          }
          case m ⇒ super.receive(m)
        }
      }).start()

      poller.poll(2000)
      latch.tryAwait(10, TimeUnit.MILLISECONDS) must be(true)
      replyLatch.tryAwait(2, TimeUnit.SECONDS) must be(true)
      bridge.stop()
      poller.dispose()
    }

    "be able to handle a SERVER_UNAVAILABLE message when sending a request" in {
      val name = "zeromq-actor-client-reply-serverunavailable-test"
      val router = context.socket(Router)
      router.bind("inproc://" + name + ".inproc")
      val poller = new ZeroMQ.ZeroMQPoller(context)
      val latch = new StandardLatch
      var bridge: ActorRef = null
      poller += (router -> ((msg: ZMessage) ⇒ {
        if (msg.messageType == "requestreply" && msg.sender.isNotBlank && msg.target == "the-target" && msg.body == ApplicationEvent('pingping).toJson) {
          val m = ZMessage("", msg.ccid, "system", "ERROR", "", "SERVER_UNAVAILABLE")
          bridge ! ProtocolMessage(m)
          latch.open()
        }
      }))
      poller.init()
      val replyLatch = new StandardLatch
      bridge = actorOf(new ZeroMqBridge(context, name) with ClientBridge with SubscriberBridge).start()
      val client = actorOf(new Actor {
        self.id = name + "-actor"
        protected def receive = {
          case 'request ⇒ {
            (bridge ? Request("the-target", ApplicationEvent('pingping))) onException {
              case e: ServerUnavailableException ⇒ replyLatch.open()
            }
          }
        }
      }).start()
      client ! 'request
      poller.poll(2000)
      poller.poll(2000)
      latch.tryAwait(10, TimeUnit.MILLISECONDS) must be(true)
      replyLatch.tryAwait(2, TimeUnit.SECONDS) must be(true)
      bridge.stop()
      poller.dispose()
    }

    "be able to handle a TIMEOUT message when sending a request" in {
      val name = "zeromq-actor-client-reply-timeout-test"
      val router = context.socket(Router)
      router.bind("inproc://" + name + ".inproc")
      val poller = new ZeroMQ.ZeroMQPoller(context)
      val latch = new StandardLatch
      var bridge: ActorRef = null
      poller += (router -> ((msg: ZMessage) ⇒ {
        if (msg.messageType == "requestreply" && msg.sender.isNotBlank && msg.target == "the-target" && msg.body == ApplicationEvent('pingping).toJson) {
          val m = ZMessage("", msg.ccid, "system", "ERROR", "", "TIMEOUT")
          bridge ! ProtocolMessage(m)
          latch.open()
        }
      }))
      poller.init()
      val replyLatch = new StandardLatch
      bridge = actorOf(new ZeroMqBridge(context, name) with ClientBridge with SubscriberBridge).start()
      val client = actorOf(new Actor {
        self.id = name + "-actor"
        protected def receive = {
          case 'request ⇒ {
            (bridge ? Request("the-target", ApplicationEvent('pingping))) onException {
              case e: RequestTimeoutException ⇒ replyLatch.open()
              case _ =>
            }
          }
        }
      }).start()
      client ! 'request
      poller.poll(2000)
      poller.poll(2000)
      latch.tryAwait(10, TimeUnit.MILLISECONDS) must be(true)
      replyLatch.tryAwait(2, TimeUnit.SECONDS) must be(true)
      bridge.stop()
      poller.dispose()
    }

  }
}