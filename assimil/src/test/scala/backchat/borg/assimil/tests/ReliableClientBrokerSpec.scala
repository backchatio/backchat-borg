package backchat
package borg
package assimil
package tests

import org.scalatest.matchers.MustMatchers
import org.zeromq.ZMQ
import org.multiverse.api.latches.StandardLatch
import Messages._
import ReliableClientBroker._
import java.util.concurrent.{ CountDownLatch, TimeUnit }
import akka.actor._
import Actor._
import org.scalatest.{ BeforeAndAfterEach, BeforeAndAfterAll, WordSpec }

object ReliableClientBrokerSpec {
  val context = ZMQ context 1

  def createServer(name: String, address: String) = {
    val server = context socket Router
    server.setIdentity((name + "-endpoint").getBytes)
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
class ReliableClientBrokerSpec extends WordSpec with MustMatchers with BeforeAndAfterAll with BeforeAndAfterEach {

  import ReliableClientBrokerSpec._

  "A ReliableClientBroker" when {

    "the bridge is ready" should {

      "connect to the servers" in {
        val name = "rel-cl-broker-connect"
        val config = DeviceConfig(context, name, "inproc://" + name + "-router.inproc")
        val brokerLatch = new StandardLatch
        val server = createServer(config.name, config.serverAddress)
        val poller = new ZeroMQ.ZeroMQPoller(context)
        val actorUuid = new Uuid()
        val connectLatch = new StandardLatch
        poller.init()
        poller += (server -> ((msg: ZMessage) ⇒ {
          connectLatch.open()
        }))
        val dev = ZeroMQ startDevice {
          new BackchatZeroMqDevice(config) with ClientActorBridge with ReliableClientBroker {
            availableServers = new AvailableServers(Map((name + "-endpoint") -> AvailableServer(config.serverAddress, 2.seconds)))
            override def init() {
              super.init()
              brokerLatch.open()
            }
          }
        }
        brokerLatch.tryAwait(3, TimeUnit.SECONDS) must be(true)
        val client = createClient(config.name)
        Ready.toZMessage.wrap(actorUuid.toString)(client)
        poller.poll(3000)
        connectLatch.tryAwait(3, TimeUnit.MILLISECONDS) must be(true)
        server.close()
        client.close()
        dev.stop
      }

      "send a ping to all servers" in {
        val name = "rel-cl-broker-first-ping"
        val config = DeviceConfig(context, name, "inproc://" + name + "-router.inproc")
        val brokerLatch = new StandardLatch
        val server = createServer(config.name, config.serverAddress)
        val poller = new ZeroMQ.ZeroMQPoller(context)
        val actorUuid = new Uuid()
        val connectLatch = new StandardLatch
        poller.init()
        poller += (server -> ((msg: ZMessage) ⇒ {
          if (msg.messageType == "system" && msg.body == "PING") connectLatch.open()
        }))
        val dev = ZeroMQ startDevice {
          new BackchatZeroMqDevice(config) with ClientActorBridge with ReliableClientBroker {
            availableServers = new AvailableServers(Map((name + "-endpoint") -> AvailableServer(config.serverAddress, 2.seconds)))
            override def init() {
              super.init()
              brokerLatch.open()
            }
          }
        }
        brokerLatch.tryAwait(3, TimeUnit.SECONDS) must be(true)
        val client = createClient(config.name)
        Ready.toZMessage.wrap(actorUuid.toString)(client)
        poller.poll(3000)
        connectLatch.tryAwait(3, TimeUnit.MILLISECONDS) must be(true)
        server.close()
        client.close()
        dev.stop
      }

    }

    "receiving a server reply" should {

      "register the server as active for a pong response" in {
        val name = "rel-cl-broker-first-pong"
        val config = DeviceConfig(context, name, "inproc://" + name + "-router.inproc")
        val brokerLatch = new StandardLatch
        val server = createServer(config.name, config.serverAddress)
        val serverPoller = new ZeroMQ.ZeroMQPoller(context)
        val actorUuid = new Uuid()
        serverPoller.init()
        serverPoller += (server -> ((msg: ZMessage) ⇒ {
          if (msg.messageType == "system" && msg.body == "PING") {
            msg.body = "PONG"
            msg.sender = name + "-endpoint"
            msg(server)
          }
        }))
        val pongLatch = new StandardLatch
        val dev = ZeroMQ startDevice {
          new BackchatZeroMqDevice(config) with ClientActorBridge with ReliableClientBroker {
            availableServers = new AvailableServers(Map((name + "-endpoint") -> AvailableServer(config.serverAddress, 2.seconds)))
            override def init() {
              super.init()
              brokerLatch.open()
            }
            override protected def inboundHandler(zmsg: ZMessage) {
              super.inboundHandler(zmsg)
              if (activeServers.size == 1) pongLatch.open()
            }
          }
        }
        brokerLatch.tryAwait(3, TimeUnit.SECONDS) must be(true)
        val client = createClient(config.name)
        Ready.toZMessage.wrap(actorUuid.toString)(client)
        serverPoller.poll(3000)
        pongLatch.tryAwait(2, TimeUnit.SECONDS) must be(true)
        server.close()
        client.close()
        dev.stop
      }

      "slide the timeouts if it's a requestreply response" in {
        val name = "rel-cl-broker-reply-slide"
        val config = DeviceConfig(context, name, "inproc://" + name + "-router.inproc")
        val brokerLatch = new StandardLatch
        val server = createServer(config.name, config.serverAddress)
        val serverPoller = new ZeroMQ.ZeroMQPoller(context)
        val actorUuid = new Uuid()
        val ccid = new Uuid()
        serverPoller.init()
        serverPoller += (server -> ((msg: ZMessage) ⇒ {
          if (msg.messageType == "system" && msg.body == "PING") {
            msg.body = "PONG"
            msg.sender = name + "-endpoint"
            msg(server)
          } else if (msg.messageType == "requestreply") {
            msg.target = msg.sender
            msg.sender = ""
            msg.body = ApplicationEvent('the_reply).toJson
            Thread.sleep(500) // simulate doing some work
            msg(server)
          }
        }))
        val pongLatch = new StandardLatch
        val replyLatch = new StandardLatch
        var oldServer: Option[ActiveServer] = None
        var newServer: Option[ActiveServer] = None
        val dev = ZeroMQ startDevice {
          new BackchatZeroMqDevice(config) with ClientActorBridge with ReliableClientBroker {
            availableServers = new AvailableServers(Map((name + "-endpoint") -> AvailableServer(config.serverAddress, 2.seconds)))
            override def init() {
              super.init()
              brokerLatch.open()
            }
            override protected def inboundHandler(zmsg: ZMessage) {
              println("*" * 140)
              println(zmsg)
              println("*" * 140)
              oldServer = activeServers.headOption
              super.inboundHandler(zmsg)
              if (zmsg.messageType == "system" && zmsg.body == "PONG") pongLatch.open()
              newServer = activeServers.headOption
              if (zmsg.messageType == "requestreply") replyLatch.open()
            }
          }
        }
        brokerLatch.tryAwait(3, TimeUnit.SECONDS) must be(true)
        val client = createClient(config.name)
        Ready.toZMessage.wrap(actorUuid.toString)(client)
        serverPoller.poll(3000)
        pongLatch.tryAwait(2, TimeUnit.SECONDS) must be(true)
        Request(config.name, ApplicationEvent('the_request), ccid, config.name + "-client").toZMessage(client)
        serverPoller.poll(3000)
        replyLatch.tryAwait(2, TimeUnit.SECONDS) must be(true)
        oldServer.get.expires.toDate.getTime must be < (newServer.get.expires.toDate.getTime)
        oldServer.get.nextPing.toDate.getTime must be < (newServer.get.nextPing.toDate.getTime)
        server.close()
        client.close()
        dev.stop
      }
    }
  }

  "when connection is idle for a while" should {

    "send the server a ping at a the rate of the configured ttl" in {
      val name = "rel-cl-broker-interval-pong"
      val config = DeviceConfig(context, name, "inproc://" + name + "-router.inproc")
      val brokerLatch = new StandardLatch
      val server = createServer(config.name, config.serverAddress)
      val serverPoller = new ZeroMQ.ZeroMQPoller(context)
      val actorUuid = new Uuid()
      val counter = new CountDownLatch(3)
      serverPoller.init()
      serverPoller += (server -> ((msg: ZMessage) ⇒ {
        if (msg.messageType == "system" && msg.body == "PING") {
          msg.body = "PONG"
          msg.sender = name + "-endpoint"
          counter.countDown()
          msg(server)
        }
      }))
      val dev = ZeroMQ startDevice {
        new BackchatZeroMqDevice(config) with ClientActorBridge with ReliableClientBroker {
          availableServers = new AvailableServers(Map((name + "-endpoint") -> AvailableServer(config.serverAddress, 500.millis)))
          override def init() {
            super.init()
            brokerLatch.open()
          }
        }
      }
      brokerLatch.tryAwait(2, TimeUnit.SECONDS) must be(true)
      val client = createClient(config.name)
      Ready.toZMessage.wrap(actorUuid.toString)(client)
      serverPoller.poll(2000)
      serverPoller.poll(2000)
      serverPoller.poll(2000)
      counter.await(6, TimeUnit.SECONDS) must be(true)
      serverPoller.dispose()
      server.close()
      client.close()
      dev.stop
    }
  }

  "the connection goes down for a little while" should {

    "buffer the requests and send when server is up" in {
      val name = "rel-cl-broker-resend-requests"
      val config = DeviceConfig(context, name, "inproc://" + name + "-router.inproc")
      val brokerLatch = new StandardLatch
      val server = createServer(config.name, config.serverAddress)
      val serverPoller = new ZeroMQ.ZeroMQPoller(context)
      val actorUuid = new Uuid()
      val reqid1 = new Uuid()
      val reqid2 = new Uuid()
      var reqCount = 0
      serverPoller.init()
      serverPoller += (server -> ((msg: ZMessage) ⇒ {
        if (msg.messageType == "system" && msg.body == "PING") {
          msg.body = "PONG"
          msg.sender = name + "-endpoint"
          msg(server)
        }
        if (msg.messageType == "requestreply") {
          msg.target = msg.sender
          msg.sender = ""
          if (msg.ccid == reqid1.toString) {
            msg.body = ApplicationEvent('the_reply).toJson
            msg(server)
          } else {
            msg.body = ApplicationEvent('the_reply2).toJson
            reqCount += 1
            //if (msg.ccid == reqid2.toString && reqCount == 1) Thread.sleep(5000)
            if (reqCount > 1) msg(server)
          }
        }
      }))
      val dev = ZeroMQ startDevice {
        new BackchatZeroMqDevice(config) with ClientActorBridge with ReliableClientBroker {
          availableServers = new AvailableServers(Map((name + "-endpoint") -> AvailableServer(config.serverAddress, 5.seconds)))
          override def init() {
            super.init()
            brokerLatch.open()
          }
        }
      }
      brokerLatch.tryAwait(2, TimeUnit.SECONDS) must be(true)

      val clientBridge = actorOf(new ZeroMqBridge(context, name) with ClientBridge {}).start()
      val counter = new CountDownLatch(2)
      val client = actorOf(new Actor {
        implicit val timeout = Actor.Timeout(15.seconds.millis)
        protected def receive = {
          case 'first ⇒ {
            val res = (clientBridge ? Request(config.name, ApplicationEvent('the_request1), reqid1, config.name + "-client")).await.result
            if (res.isDefined) counter.countDown()
          }
          case 'second ⇒ {
            val res = (clientBridge ? Request(config.name, ApplicationEvent('the_request2), reqid2, config.name + "-client")).await.result
            if (res.isDefined) counter.countDown()
          }
        }
      }).start()
      serverPoller.poll(2000)
      client ! 'first
      serverPoller.poll(2000)
      client ! 'second
      serverPoller.poll(2000)
      serverPoller.poll(2000)
      serverPoller.poll(2000)
      serverPoller.poll(2000)
      serverPoller.poll(2000)
      serverPoller.poll(2000)
      counter.await(5, TimeUnit.SECONDS) must be(true)
      reqCount must be(2)
      client.stop()
      clientBridge.stop()
      serverPoller.dispose()
      server.close()
      dev.stop
    }

    "not send more than one ping" in {
      val name = "rel-cl-broker-only-1-ping-1"
      val config = DeviceConfig(context, name, "inproc://" + name + "-router.inproc")
      val brokerLatch = new StandardLatch
      val server = createServer(config.name, config.serverAddress)
      val serverPoller = new ZeroMQ.ZeroMQPoller(context)
      val actorUuid = new Uuid()
      var counter = 0
      serverPoller.init()
      serverPoller += (server -> ((msg: ZMessage) ⇒ {
        if (msg.messageType == "system" && msg.body == "PING" && counter < 3) {
          msg.body = "PONG"
          msg.sender = name + "-endpoint"
          msg(server)
        }
        counter += 1
      }))
      val dev = ZeroMQ startDevice {
        new BackchatZeroMqDevice(config) with ClientActorBridge with ReliableClientBroker {
          availableServers = new AvailableServers(Map((name + "-endpoint") -> AvailableServer(config.serverAddress, 1.seconds)))
          override def init() {
            super.init()
            brokerLatch.open()
          }
        }
      }
      brokerLatch.tryAwait(2, TimeUnit.SECONDS) must be(true)
      val client = createClient(config.name)
      Ready.toZMessage.wrap(actorUuid.toString)(client)
      serverPoller.poll(1500)
      serverPoller.poll(1500)
      serverPoller.poll(1500)
      serverPoller.poll(1500)
      serverPoller.poll(1500)
      counter must be(4)
      serverPoller.dispose()
      server.close()
      client.close()
      dev.stop
    }

  }

  "the server goes down for a long period of time" should {
    "notify the clients that the server is unavailable" in {
      val name = "rel-cl-broker-down-long-1"
      val config = DeviceConfig(context, name, "inproc://" + name + "-router.inproc")
      val brokerLatch = new StandardLatch
      val server = createServer(config.name, config.serverAddress)
      val serverPoller = new ZeroMQ.ZeroMQPoller(context)
      val actorUuid = new Uuid()
      var counter = 0
      serverPoller.init()
      serverPoller += (server -> ((msg: ZMessage) ⇒ {
        if (msg.messageType == "system" && msg.body == "PING" && counter < 3) {
          msg.body = "PONG"
          msg.sender = name + "-endpoint"
          msg(server)
        }
        counter += 1
      }))
      val dev = ZeroMQ startDevice {
        new BackchatZeroMqDevice(config) with ClientActorBridge with ReliableClientBroker {
          availableServers = new AvailableServers(Map((name + "-endpoint") -> AvailableServer(config.serverAddress, 1.second)))
          override def init() {
            super.init()
            brokerLatch.open()
          }
        }
      }
      brokerLatch.tryAwait(2, TimeUnit.SECONDS) must be(true)
      // Setup actor
      val handlerLatch = new StandardLatch
      val client = actorOf(new ZeroMqBridge(context, name) with ClientBridge {
        override protected def receive = {
          case ProtocolMessage(_, _, _, _, "SERVER_UNAVAILABLE") ⇒ {
            handlerLatch.open()
          }
        }
      }).start()
      serverPoller.poll(1500) // READY
      serverPoller.poll(1500) // PING 1   REPLY
      serverPoller.poll(1500) // PING 2   REPLY
      serverPoller.poll(1500) // PING 3   REPLY
      serverPoller.poll(1500) // PING 4   NOREPLY
      handlerLatch.tryAwait(2, TimeUnit.SECONDS) must be(true)
      serverPoller.dispose()
      server.close()
      client.stop()
      dev.stop
    }

    "have the max value as response time" in {
      // I can't figure out testing this short of spawning processes and checking exit statuses
      pending
    }
  }

  "the server is disconnected" should {
    "reply to client with server unavailable when there are no servers available" in {
      val name = "rel-cl-broker-no-server"
      val config = DeviceConfig(context, name, "inproc://" + name + "-router.inproc")
      val brokerLatch = new StandardLatch
      val server = createServer(config.name, config.serverAddress)
      val serverPoller = new ZeroMQ.ZeroMQPoller(context)
      val actorUuid = new Uuid()
      var counter = 0
      serverPoller.init()
      serverPoller += (server -> ((msg: ZMessage) ⇒ {
        if (msg.messageType == "system" && msg.body == "PING" && counter < 3) {
          msg.body = "PONG"
          msg.sender = name + "-endpoint"
          msg(server)
        }
        counter += 1
      }))
      val dev = ZeroMQ startDevice {
        new BackchatZeroMqDevice(config) with ClientActorBridge with ReliableClientBroker {
          availableServers = new AvailableServers()
          override def init() {
            super.init()
            brokerLatch.open()
          }
        }
      }
      brokerLatch.tryAwait(2, TimeUnit.SECONDS) must be(true)
      // Setup actor
      val handlerLatch = new StandardLatch
      val client = actorOf(new ZeroMqBridge(context, name) with ClientBridge {
        override protected def receive = {
          case ProtocolMessage(_, _, _, _, "SERVER_UNAVAILABLE") ⇒ {
            handlerLatch.open()
          }
        }
      }).start()
      handlerLatch.tryAwait(2, TimeUnit.SECONDS) must be(true)
      serverPoller.dispose()
      server.close()
      client.stop()
      dev.stop
    }

  }

}