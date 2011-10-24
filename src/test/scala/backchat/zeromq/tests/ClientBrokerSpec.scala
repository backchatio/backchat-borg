package backchat
package zeromq
package tests

import org.scalatest.WordSpec
import org.scalatest.matchers.MustMatchers
import org.multiverse.api.latches.StandardLatch
import java.util.concurrent.TimeUnit
import org.zeromq.ZMQ
import org.zeromq.ZMQ.Poller

object ClientBrokerSpec {
  val context = ZMQ.context(1)

  def createServer(address: String) = {
    val server = context socket Router
    server.bind(address)
    server
  }

  def createClient(name: String) = {
    val client = context socket Dealer
    client.setIdentity((name + "-client").getBytes("UTF-8"))
    client.connect("inproc://" + name + ".inproc")
    client.setLinger(0L)
    client
  }
}
class ClientBrokerSpec extends WordSpec with MustMatchers {

  import ClientBrokerSpec._

  "A ClientBroker" should {
    "start without problems" in {
      val config = DeviceConfig(context, "client-broker-start", "inproc://server-router.inproc")
      val server = createServer(config.serverAddress)
      val latch = new StandardLatch
      val dev = ZeroMQ startDevice (new BackchatZeroMqDevice(config) with ClientActorBridge with ClientBroker {
        val outboundAddress = config.serverAddress
        override def execute() = {
          latch.open()
          true
        }
      })
      latch.tryAwait(3, TimeUnit.SECONDS) must be(true)
      dev.stop
    }

    "wrap an enqueue command into a ZMessage envelope" in {
      val config = DeviceConfig(context, "client-broker-enqueue", "inproc://server-router-enqueue.inproc")
      val server = createServer(config.serverAddress)
      val ccid = newCcId
      val brokerLatch = new StandardLatch
      val latch = new StandardLatch
      val poller = new ZeroMQ.ZeroMQPoller(context)
      poller += (server -> ((msg: ZMessage) ⇒ {
        if (msg.ccid == ccid && msg.messageType == "fireforget" && msg.target == "the-target") latch.open()
      }))
      poller.init()
      val dev = ZeroMQ startDevice {
        new BackchatZeroMqDevice(config) with ClientActorBridge with ClientBroker {
          val outboundAddress = config.serverAddress
          override def init() {
            super.init()
            brokerLatch.open()
          }
        }
      }
      brokerLatch.tryAwait(3, TimeUnit.SECONDS) must be(true)
      val client = createClient(config.name)
      ZMessage("", ccid, "fireforget", "", "the-target", ApplicationEvent('pong).toJson)(client)
      poller.poll(2000)
      latch.tryAwait(10, TimeUnit.MILLISECONDS) must be(true)
      dev.stop
    }

    "wrap a request reply command in a ZMessage envelope" in {
      val config = DeviceConfig(context, "client-broker-request", "inproc://server-router-request.inproc")
      val server = createServer(config.serverAddress)
      val ccid = newCcId
      val brokerLatch = new StandardLatch
      val poller = new ZeroMQ.ZeroMQPoller(context)
      val latch = new StandardLatch
      poller += (server -> ((msg: ZMessage) ⇒ {
        if (msg.ccid == ccid && msg.messageType == "requestreply" && msg.target == "the-target") {
          msg.target = msg.sender
          msg.sender = ""
          msg(server)
          latch.open()
        }
      }))
      poller.init()
      val dev = ZeroMQ startDevice {
        new BackchatZeroMqDevice(config) with ClientActorBridge with ClientBroker {
          val outboundAddress = config.serverAddress
          override def init() {
            super.init()
            brokerLatch.open()
          }
        }
      }
      brokerLatch.tryAwait(3, TimeUnit.SECONDS) must be(true)
      val client = createClient(config.name)
      val cp = context poller 1
      cp.register(client, Poller.POLLIN)
      ZMessage("", ccid, "requestreply", config.name + "-client", "the-target", ApplicationEvent('pong).toJson)(client)
      poller.poll(2000)
      latch.tryAwait(2, TimeUnit.SECONDS) must be(true)
      cp.poll(3000L)
      cp.pollin(0) must be(true)
      val repl = ZMessage(client)
      repl.messageType must equal("requestreply")
      repl.ccid must equal(ccid)
      repl.target must equal(config.name + "-client")
      repl.body must equal("[\"pong\"]")
      repl.sender must be('empty)
      dev.stop
    }

//    "publish a message to a subscriber" in {
//      val config = DeviceConfig(context, "client-broker-publish", "inproc://server-router-publish.inproc")
//      val brokerLatch = new StandardLatch
//      val server = createServer(config.serverAddress)
//      val dev = ZeroMQ startDevice {
//        new BackchatZeroMqDevice(config) with ClientActorBridge with ClientBroker with PubSubPublisher {
//          val outboundAddress = config.serverAddress
//          override def init() {
//            super.init()
//            brokerLatch.open()
//          }
//        }
//      }
//      brokerLatch.tryAwait(3, TimeUnit.SECONDS) must be (true)
//      val subscriber = context.socket(Sub)
//      subscriber.connect(config.pubsubAddress)
//      subscriber.subscribe("the-topic".getBytes("UTF-8"))
//      val poller = new ZeroMQ.ZeroMQPoller(context)
//      val subscriberLatch = new StandardLatch
//      poller += (subscriber -> ((msg: ZMessage) => {
//        println("the message: " + msg)
//        if(msg.address == "the-topic" && msg.body == ApplicationEvent('ping).toJson)
//          subscriberLatch.open()
//      }))
//      poller.init()
//      val client = createClient(config.name)
//      ZMessage("", newCcId, "pubsub", "publish", "the-topic", ApplicationEvent('ping).toJson)(client)
//      poller.poll(2000)
//      subscriberLatch.tryAwait(10, TimeUnit.MILLISECONDS) must be (true)
//    }
  }
}