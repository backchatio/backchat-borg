package backchat
package borg
package zeromq
package tests

import org.scalatest.WordSpec
import org.scalatest.matchers.MustMatchers
import org.zeromq.ZMQ
import org.multiverse.api.latches.StandardLatch
import java.util.concurrent.TimeUnit
import org.joda.time.Duration
import akka.actor.Uuid

object BackchatZeroMqClientSpec {
  val context = ZMQ context 1
}
class BackchatZeroMqClientSpec extends WordSpec with MustMatchers {

  import BackchatZeroMqClientSpec._

  "A ZeroMQClient" should {

    "be able to enqueue a message" in {
      val name = "zeromq-client-test"
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
      val client = new BackchatZeroMqClient(name + "-client", context, name)
      client.enqueue("the-target", ApplicationEvent('pingping))
      poller.poll(2000)
      latch.tryAwait(10, TimeUnit.MILLISECONDS) must be(true)
    }

    //    "be able to publish a message" in {
    //      val name = "zeromq-client-publish-test"
    //      val router = context.socket(Router)
    //      router.bind("inproc://" + name + ".inproc")
    //      val poller = new ZeroMQ.ZeroMQPoller(context)
    //      val latch = new StandardLatch
    //      poller += (router -> ((msg: ZMessage) => {
    //        if(msg.messageType == "pubsub" && msg.addresses.last == "the-topic" && msg.body == ApplicationEvent('pingping).toJson) {
    //          latch.open()
    //        }
    //      }))
    //      poller.init()
    //      val client = new BackchatZeroMqClient(name + "-client", context, name)
    //      client.publish("the-topic", ApplicationEvent('pingping))
    //      poller.poll(2000)
    //      latch.tryAwait(10, TimeUnit.MILLISECONDS) must be (true)
    //    }

    "be able to request a message with a reply" in {
      val name = "zeromq-client-reply-test"
      val config = DeviceConfig(context, name, "inproc://blahblah.inproc")
      val latch = new StandardLatch
      val router = ZeroMQ startDevice {
        new BackchatZeroMqDevice(config) {
          val sock = context.socket(Router)
          poller += (sock -> (send _))
          override def send(zmsg: ZMessage) {
            if (zmsg.messageType == "requestreply" &&
              zmsg.sender == (name + "-client") &&
              zmsg.body == ApplicationEvent('pingping).toJson) {
              zmsg(sock)
            }
          }
          override def init() {
            super.init()
            sock.bind("inproc://" + name + ".inproc")
            latch.open()
          }
          override def dispose() {
            sock.close()
            super.dispose()
          }
        }
      }
      latch.tryAwait(2, TimeUnit.SECONDS) must be(true)
      val client = new BackchatZeroMqClient(name + "-client", context, name)
      val replyLatch = new StandardLatch
      client.request("the-target", ApplicationEvent('pingping)) { evt ⇒
        evt must equal(ApplicationEvent('pingping))
        replyLatch.open()
      }
      replyLatch.tryAwait(2, TimeUnit.SECONDS) must be(true)
      router.stop
    }

    "should not block forever if a request doesn't get a reply" in {

      val name = "zeromq-client-exception-test"
      val config = DeviceConfig(context, name, "inproc://blahblah-exc.inproc")
      val latch = new StandardLatch
      val router = ZeroMQ startDevice {
        new BackchatZeroMqDevice(config) {
          val sock = context.socket(Router)
          poller += (sock -> (send _))
          override def send(zmsg: ZMessage) {

          }
          override def init() {
            super.init()
            sock.bind("inproc://" + name + ".inproc")
            latch.open()
          }
          override def dispose() {
            sock.close()
            super.dispose()
          }
        }
      }
      latch.tryAwait(2, TimeUnit.SECONDS) must be(true)
      val client = new BackchatZeroMqClient(name + "-client", context, name, new Duration(200))
      val replyLatch = new StandardLatch
      evaluating {
        client.request("the-target", ApplicationEvent('pingping)) { evt ⇒
          evt must equal(ApplicationEvent('pingping))
          replyLatch.open()
        }
      } must produce[RequestTimeoutException]
      router.stop
    }

    "throw a ServerUnavailableException when the backend responds with that" in {
      val name = "zeromq-client-serverunavailable-test"
      val config = DeviceConfig(context, name, "inproc://" + name + "-router.inproc")
      val latch = new StandardLatch
      val router = ZeroMQ startDevice {
        new BackchatZeroMqDevice(config) {
          val sock = context.socket(Router)
          poller += (sock -> (send _))
          override def send(zmsg: ZMessage) {
            val msg = Messages.Error(new Uuid(zmsg.ccid), "SERVER_UNAVAILABLE").toZMessage
            msg.addresses = zmsg.addresses
            msg(sock)
          }
          override def init() {
            super.init()
            sock.bind("inproc://" + name + ".inproc")
            latch.open()
          }
          override def dispose() {
            sock.close()
            super.dispose()
          }
        }
      }
      latch.tryAwait(2, TimeUnit.SECONDS) must be(true)
      val client = new BackchatZeroMqClient(name + "-client", context, name, new Duration(5000))
      evaluating {
        client.request("the-target", ApplicationEvent('pingping)) { evt ⇒
          // never get here
        }
      } must produce[ServerUnavailableException]
      router.stop
    }

    "throw a RequestTimeoutException when the backend responds with that" in {
      val name = "zeromq-client-timeout-test"
      val config = DeviceConfig(context, name, "inproc://" + name + "-router.inproc")
      val latch = new StandardLatch
      val router = ZeroMQ startDevice {
        new BackchatZeroMqDevice(config) {
          val sock = context.socket(Router)
          poller += (sock -> (send _))
          override def send(zmsg: ZMessage) {
            val msg = Messages.Error(new Uuid(zmsg.ccid), "TIMEOUT").toZMessage
            msg.addresses = zmsg.addresses
            msg(sock)
          }
          override def init() {
            super.init()
            sock.bind("inproc://" + name + ".inproc")
            latch.open()
          }
          override def dispose() {
            sock.close()
            super.dispose()
          }
        }
      }
      latch.tryAwait(2, TimeUnit.SECONDS) must be(true)
      val client = new BackchatZeroMqClient(name + "-client", context, name, new Duration(5000))
      evaluating {
        client.request("the-target", ApplicationEvent('pingping)) { evt ⇒
          // never get here
        }
      } must produce[RequestTimeoutException]
      router.stop
    }

  }
}