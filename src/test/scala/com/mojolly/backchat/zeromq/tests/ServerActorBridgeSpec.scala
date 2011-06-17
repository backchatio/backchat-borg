package com.mojolly.backchat
package zeromq
package tests

import org.scalatest.matchers.MustMatchers
import org.multiverse.api.latches.StandardLatch
import org.zeromq.ZMQ
import LibraryImports._
import java.util.concurrent.{ CountDownLatch, TimeUnit }
import akka.actor.{ Actor, Uuid }
import queue.ApplicationEvent
import collection.mutable.ListBuffer
import org.scalatest.{ BeforeAndAfterAll, BeforeAndAfterEach, WordSpec }

object ServerActorBridgeSpec {
  val context = ZMQ.context(1)
}
class ServerActorBridgeSpec extends WordSpec with MustMatchers with BeforeAndAfterAll {

  //  val config = DeviceConfig(SimpleBrokerSpec.context, "simple-broker-example", "tcp://*:6256", "tcp://*:6257")
  //  ZeroMQ.trace = true

  def createBridgeSocket(id: String, name: String, context: Context) = {
    val br = context.socket(Dealer)
    br.setIdentity(id.getBytes(Utf8))
    br.connect("inproc://" + name + ".inproc")
    br.setLinger(0L)
    br
  }
  "A BackchatZeroMqDevice with ServerActorBridge" should {

    "Start without problems" in {
      val latch = new StandardLatch
      val config = DeviceConfig(ServerActorBridgeSpec.context, "simple-broker-example", "tcp://*:6256")
      val dev = ZeroMQ startDevice (new BackchatZeroMqDevice(config) with ServerActorBridge {
        val routerAddress = "inproc://router.test.inproc"
        override def execute() = {
          latch.open()
          true
        }
      })
      latch.tryAwait(3, TimeUnit.SECONDS) must be(true)
      dev.stop
    }

    "Register the actor as handler on 'READY' message" in {
      val config = DeviceConfig(ServerActorBridgeSpec.context, "simple-broker-example", "tcp://*:6256")
      val latch = new CountDownLatch(2)
      val l2 = new StandardLatch
      val actorId = new Uuid().toString
      val name = "the-ready-test"
      val dev = ZeroMQ startDevice (new BackchatZeroMqDevice(config.copy(name = name)) with ServerActorBridge {
        val routerAddress = config.serverAddress
        override def init() { super.init(); l2.open }
        override protected def setHandler(msg: String) {
          if (msg == actorId) latch.countDown()
          super.setHandler(msg)
          if (handler == Some(new Uuid(actorId))) latch.countDown()
        }
      })
      l2.tryAwait(2, TimeUnit.SECONDS) must be(true)
      val bridge = createBridgeSocket(actorId, name, config.context)
      ZMessage(actorId, "", "", "system", "", "", "READY")(bridge)
      latch.await(2, TimeUnit.SECONDS) must be(true)
      bridge.close()
      dev.stop
    }

    "Unregister the actor as handler on 'STOPPING' message" in {
      val config = DeviceConfig(ServerActorBridgeSpec.context, "simple-broker-example", "tcp://*:6258")
      val latch = new CountDownLatch(2)
      val l2 = new StandardLatch
      val actorId = new Uuid().toString
      val name = "the-stopping-test"
      val dev = ZeroMQ startDevice (new BackchatZeroMqDevice(config.copy(name = name)) with ServerActorBridge {
        val routerAddress = config.serverAddress
        override def init() { super.init(); l2.open }
        override protected def clearHandler(msg: String) {
          if (msg == actorId) latch.countDown()
          super.clearHandler(msg)
          if (handler == None) latch.countDown()
        }
      })
      l2.tryAwait(2, TimeUnit.SECONDS) must be(true)
      val bridge = createBridgeSocket(actorId, name, config.context)
      ZMessage(actorId, "", "", "system", "", "", "STOPPING")(bridge)
      latch.await(2, TimeUnit.SECONDS) must be(true)
      bridge.close()
      dev.stop
    }

    "forward a request reply request message to the actor handler" in {
      val latch = new StandardLatch
      val messageBody: String = ApplicationEvent('ping).toJson
      val clientId = "the-request-client"
      val name = "the-request-test"
      val msg = ZMessage(newCcId, "requestreply", "", clientId, messageBody)
      val handlerActor = Actor.actorOf(new Actor {
        protected def receive = {
          case x: ProtocolMessage ⇒ {
            if (x == ProtocolMessage(new Uuid(msg.ccid), "requestreply", None, clientId, messageBody)) latch.open()
          }
        }
      }).start()
      val config = DeviceConfig(ServerActorBridgeSpec.context, name, "tcp://*:6260")
      val dev = ZeroMQ startDevice (new BackchatZeroMqDevice(config) with ServerActorBridge {
        handler = Some(handlerActor.uuid)
        val routerAddress = config.serverAddress
      })
      val client = ServerActorBridgeSpec.context.socket(Req)
      client.setIdentity(clientId.getBytes(Utf8))
      client.connect("tcp://localhost:6260")
      client.setLinger(0)
      msg(client)
      latch.tryAwait(2, TimeUnit.SECONDS) must be(true)
      client.close()
      dev.stop
    }

    "forward a request reply response message to the client" in {
      val latch = new StandardLatch
      val requestLatch = new StandardLatch
      val name = "the-reply-test"
      val config = DeviceConfig(ServerActorBridgeSpec.context, name, "tcp://*:6262")
      val clientId = "the-reply-client"
      val handlerActor = Actor.actorOf(new Actor {
        protected def receive = {
          case x: ProtocolMessage ⇒ {
            requestLatch.open()
          }
        }
      }).start()
      val actorId = handlerActor.uuid.toString
      val dev = ZeroMQ startDevice (new BackchatZeroMqDevice(config) with ServerActorBridge {
        handler = Some(handlerActor.uuid)
        val routerAddress = config.serverAddress
        override def init() { super.init(); latch.open() }
      })
      latch.tryAwait(2, TimeUnit.SECONDS) must be(true)
      val client = config.context.socket(Req)
      client.setIdentity(clientId.getBytes(Utf8))
      client.connect("tcp://localhost:6262")
      client.setLinger(1)
      val bridge = createBridgeSocket(actorId, name, config.context)
      ZMessage("requestreply", "", "", "hello")(client)
      requestLatch.tryAwait(2, TimeUnit.SECONDS) must be(true)
      ZMessage(clientId, "", "requestreply", "", "", "hello")(bridge)
      ZMessage(client).body must equal("hello")
      bridge.close()
      dev.stop
    }

  }

  "A BackchatZeroMqDevice with ServerActorBridge and PingPongResponder" should {
    "respond to ping messages sent from clients" in {
      val latch = new StandardLatch
      val requestLatch = new StandardLatch
      val name = "the-pingpong-response-test"
      val config = DeviceConfig(ServerActorBridgeSpec.context, name, "inproc://" + name + "-server.inproc")
      val clientId = "the-pingpong-response-client"
      val dev = ZeroMQ startDevice (new BackchatZeroMqDevice(config) with ServerActorBridge with PingPongResponder {
        val routerAddress = config.serverAddress
        override def init() { super.init(); latch.open() }
      })
      latch.tryAwait(2, TimeUnit.SECONDS) must be(true)
      val client = config.context.socket(Req)
      client.setIdentity(clientId.getBytes(Utf8))
      client.connect(config.serverAddress)
      val poller = new ZeroMQ.ZeroMQPoller(config.context)
      poller += (client -> ((msg: ZMessage) ⇒ {
        if (msg.body == "PONG" && msg.sender == (name + "-endpoint"))
          requestLatch.open()
      }))
      poller.init()

      ZMessage(clientId, "", "system", "", "", "PING")(client)
      poller.poll(2000)
      requestLatch.tryAwait(10, TimeUnit.MILLISECONDS) must be(true)
      poller.dispose()
      client.close()
      dev.stop
    }

    "forward ping messages to the pingpong observers" in {
      val latch = new StandardLatch
      val requestLatch = new StandardLatch
      val name = "the-pingpong-handler-test"
      val config = DeviceConfig(ServerActorBridgeSpec.context, name, "inproc://" + name + "-server.inproc")
      val clientId = "the-pingpong-handler-client"
      val handlerActor = Actor.actorOf(new Actor with PingPongObserver {
        protected def receive = {
          case "IGNOREALL" ⇒
        }
        protected def onClientPing(cl: Array[Byte]) {
          if (new String(cl, Utf8) == clientId) requestLatch.open()
        }
      }).start()
      val dev = ZeroMQ startDevice (new BackchatZeroMqDevice(config) with ServerActorBridge with PingPongResponder {
        val routerAddress = config.serverAddress
        override def init() { super.init(); latch.open() }
      })
      latch.tryAwait(2, TimeUnit.SECONDS) must be(true)
      val client = config.context.socket(Req)
      client.setIdentity(clientId.getBytes(Utf8))
      client.connect(config.serverAddress)
      ZMessage(clientId, "", "system", "", "", "PING")(client)
      requestLatch.tryAwait(2, TimeUnit.SECONDS) must be(true)
      handlerActor.stop()
      client.close()
      dev.stop
    }
  }
}