package com.mojolly.backchat
package zeromq
package tests

import org.scalatest.matchers.MustMatchers
import akka.actor._
import Actor._
import akka.util.duration._
import collection.immutable.Queue
import com.mojolly.backchat.zeromq._
import org.zeromq.ZMQ
import queue.ApplicationEvent
import org.scalatest.{ BeforeAndAfterAll, WordSpec }
import org.zeromq.ZMQ.Poller
import akka.testkit._

object ServiceRegistryBridgeSpec {
  val context = ZMQ context 1
}
class ServiceRegistryBridgeSpec extends WordSpec with MustMatchers with TestKit with BeforeAndAfterAll {

  import ServiceRegistryBridgeSpec._

  override protected def afterAll() {
    stopTestActor
  }

  val ccid = new Uuid()
  val appEvtMatch: PartialFunction[Any, Boolean] = {
    case ApplicationEvent('the_request, _) ⇒ true
  }

  "A ZeroMq Server bridge with registry bridge" should {
    "dispatch requests with replies through the registry" in {
      val ccid = new Uuid()
      val bridge = actorOf(new ZeroMqBridge(context, "the-dev-name") with ServerBridge with ServiceRegistryBridge {
        override def preStart = {}
      }).start()
      val sut = actorOf(new ServiceRegistry(Map("message_channels" -> Queue(testActor.id)))).start()
      within(2.seconds) {
        bridge ! ProtocolMessage(ccid, "requestreply", Some("the-client"), "message_channels", "[\"the_request\"]")
        expectMsg(appEvtMatch)
      }
      bridge.stop()
      sut.stop()
    }
    "dispatch enqueue events through the registry" in {
      val ccid = new Uuid()
      val bridge = actorOf(new ZeroMqBridge(context, "the-dev-name") with ServerBridge with ServiceRegistryBridge {
        override def preStart = {}
      }).start()
      val sut = actorOf(new ServiceRegistry(Map("message_channels" -> Queue(testActor.id)))).start()
      within(2.seconds) {
        bridge ! ProtocolMessage(ccid, "fireforget", None, "message_channels", "[\"the_request\"]")
        expectMsg(appEvtMatch)
      }
      bridge.stop()
      sut.stop()
    }
    "reply to hello with capabilities" in {
      val ccid = new Uuid()
      val name = "bridge-hello-reply"
      val proxy = context socket Dealer
      proxy.bind("inproc://" + name + ".inproc")
      val poller = context poller 1
      poller.register(proxy, Poller.POLLIN)
      val bridge = actorOf(new ZeroMqBridge(context, name) with ServerBridge with ServiceRegistryBridge).start()
      val sut = actorOf(new ServiceRegistry(Map("message_channels" -> Queue(testActor.id), "streams" -> Queue("blah")))).start()
      bridge ! ProtocolMessage(ccid, "system", Some("HELLO"), "", "")
      poller.poll(2.seconds.toMicros) // lose the ready message
      ZMessage(proxy)
      poller.poll(2.seconds.toMicros)
      poller.pollin(0) must be(true)
      val msg = ZMessage(proxy)
      msg.sender must equal("CAPABILITIES")
      msg.target must equal("1.0")
      msg.body must equal("""["message_channels","streams"]""")
      poller unregister proxy
      proxy.close()
      bridge.stop()
      sut.stop()
      stopTestActor
    }
  }
}