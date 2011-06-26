package com.mojolly.backchat
package zeromq
package tests

import akka.actor._
import akka.actor.Actor._
import org.scalatest.{ BeforeAndAfterAll, GivenWhenThen, FeatureSpec }
import org.zeromq.ZMQ
import net.liftweb.json._
import org.multiverse.api.latches.StandardLatch
import Messages._
import java.util.concurrent.{ CountDownLatch, TimeUnit }
import zeromq.ReliableClientBroker.{ AvailableServer, AvailableServers }
import org.scala_tools.time.Imports._

object ZeroMqIntegrationSpec {
  val context = ZMQ context 1

}
class ZeroMqIntegrationSpec extends FeatureSpec with GivenWhenThen with BeforeAndAfterAll {

  import ZeroMqIntegrationSpec._

  val clientConfig = DeviceConfig(context, "zmq-integration-client-broker", "inproc://integration-server-broker.inproc")
  //  val zeromqClientDevice = new BackchatZeroMqDevice(config) with

  feature("An actor client can communicate with the backend") {

    info("As a 0mq actor client")
    info("I want to be able to communicate with the backend")
    info("And I want to be able to publish events")
    info("And I want to be able to subscribe to events")

    scenario("the actor enqueues a message") {
      given("an initialized system")
      val clientConfig = DeviceConfig(context, "integr-client-actor-enqueue-test", "inproc://integr-a-aaa.inproc")
      val serverConfig = clientConfig.copy(name = "integr-server-actor-enqueue-test")
      val serverLatch = new StandardLatch
      val clientLatch = new StandardLatch
      val serverBroker = ZeroMQ startDevice {
        new BackchatZeroMqDevice(serverConfig) with ServerActorBridge {
          val routerAddress = serverConfig.serverAddress
          override def init() {
            super.init()
            serverLatch.open()
          }
        }
      }
      assert(serverLatch.tryAwait(2, TimeUnit.SECONDS))
      val clientBroker = ZeroMQ startDevice {
        new BackchatZeroMqDevice(clientConfig) with ClientActorBridge with ClientBroker {
          val outboundAddress = clientConfig.serverAddress
          override def init() {
            super.init()
            clientLatch.open()
          }
        }
      }
      val serverActorName = serverConfig.name + "-handler"
      val clientName = clientConfig.name + "-handler"
      val latch = new StandardLatch
      val serverstarted = new StandardLatch
      val serverActor = actorOf(new Actor {
        self.id = serverActorName

        override def preStart {
          self ! 'openLatch
        }

        protected def receive = {
          case 'openLatch                                  ⇒ serverstarted.open()
          case ApplicationEvent('enqueued_event, JNothing) ⇒ latch.open()
        }
      }).start()
      val serverBridge = actorOf(new ZeroMqBridge(context, serverConfig.name) with ServerBridge).start()
      assert(clientLatch.tryAwait(2, TimeUnit.SECONDS))
      val client = actorOf(new ZeroMqBridge(context, clientConfig.name) with ClientBridge).start()
      when("an actor enqueues a message")
      assert(serverstarted.tryAwait(2, TimeUnit.SECONDS))
      client ! Enqueue(serverActorName, ApplicationEvent('enqueued_event))
      then("the server actor should receive the message")
      assert(latch.tryAwait(2, TimeUnit.SECONDS))
      serverBroker.stop
      clientBroker.stop
      serverActor.stop()
      client.stop()
    }

    scenario("the actor makes a request") {
      given("an initialized system")
      val clientConfig = DeviceConfig(context, "integr-client-actor-reply-test", "inproc://integr-a-ccc.inproc")
      val serverConfig = clientConfig.copy(name = "integr-server-actor-reply-test")
      val serverLatch = new StandardLatch
      val clientLatch = new StandardLatch
      val serverBroker = ZeroMQ startDevice {
        new BackchatZeroMqDevice(serverConfig) with ServerActorBridge {
          val routerAddress = serverConfig.serverAddress
          override def init() {
            super.init()
            serverLatch.open()
          }
        }
      }
      assert(serverLatch.tryAwait(2, TimeUnit.SECONDS))
      val clientBroker = ZeroMQ startDevice {
        new BackchatZeroMqDevice(clientConfig) with ClientActorBridge with ClientBroker {
          val outboundAddress = clientConfig.serverAddress
          override def init() {
            super.init()
            clientLatch.open()
          }
        }
      }
      val serverActorName = serverConfig.name + "-handler"
      val clientName = clientConfig.name + "-handler"
      val latch = new StandardLatch
      val serverStarted = new StandardLatch
      val serverActor = actorOf(new Actor {
        self.id = serverActorName

        override def preStart {
          self ! 'openLatch
        }

        protected def receive = {
          case 'openLatch ⇒ {
            serverStarted.open()
          }
          case ApplicationEvent('a_client_request, JNothing) ⇒ {
            self.sender foreach { _ ! JsonParser.parse(ApplicationEvent('a_server_reply).toJson) }
          }
        }
      }).start()
      val serverBridge = actorOf(new ZeroMqBridge(context, serverConfig.name) with ServerBridge).start()
      assert(clientLatch.tryAwait(2, TimeUnit.SECONDS))
      val clientBridge = actorOf(new ZeroMqBridge(context, clientConfig.name) with ClientBridge).start()
      val replyLatch = new StandardLatch
      val client = actorOf(new Actor {
        protected def receive = {
          case 'request ⇒ {
            val repl = clientBridge !! Request(serverActorName, ApplicationEvent('a_client_request))
            if (repl.map(_.asInstanceOf[ApplicationEvent]).forall(_.action == 'a_server_reply)) replyLatch.open()
          }
        }
      }).start()
      when("an actor makes a request")
      assert(serverStarted.tryAwait(2, TimeUnit.SECONDS))
      client ! 'request
      then("the client actor should receive the response")
      assert(replyLatch.tryAwait(2, TimeUnit.SECONDS))
      client.stop()
      serverActor.stop()
      serverBridge.stop()
      clientBridge.stop()
      clientBroker.stop
      serverBroker.stop
    }
    //
    //    scenario("the actor publishes a message") {
    //      given("an initialized system")
    //      when("an client actor publishes a message")
    //      then("the server actor should receive the message")
    //      pending
    //    }

    scenario("the actor subscribes to a topic") {
      val clientConfig = DeviceConfig(context, "integr-client-actor-subscribe-test", "inproc://integr-a-eee.inproc")
      val serverConfig = clientConfig.copy(name = "integr-server-actor-subscribe-test")
      val serverLatch = new StandardLatch
      val clientLatch = new StandardLatch
      val subscribeLatch = new StandardLatch
      val serverBroker = ZeroMQ startDevice {
        new BackchatZeroMqDevice(serverConfig) with ServerActorBridge with ServerPubSubPublisher {

          val routerAddress = serverConfig.serverAddress
          override def init() {
            super.init()
            serverLatch.open()
          }

          override protected def inboundHandler(zmsg: ZMessage) = {
            super.inboundHandler(zmsg)
            if (zmsg.messageType == "pubsub" && zmsg.sender == "subscribe")
              subscribeLatch.open()
          }
        }
      }
      assert(serverLatch.tryAwait(2, TimeUnit.SECONDS))
      val clientBroker = ZeroMQ startDevice {
        new BackchatZeroMqDevice(clientConfig) with ClientActorBridge with ClientBroker with ClientPubSubSubscriber {
          val outboundAddress = clientConfig.serverAddress
          override def init() {
            super.init()
            clientLatch.open()
          }
        }
      }
      val serverActorName = serverConfig.name + "-handler"
      val clientName = clientConfig.name + "-handler"
      assert(clientLatch.tryAwait(2, TimeUnit.SECONDS))
      val bridgeLatch = new CountDownLatch(2)
      val serverBridge = actorOf(new ZeroMqBridge(context, serverConfig.name) with ServerBridge with PublisherBridge {
        override def preStart {
          super.preStart
          self ! 'openLatch
        }
        protected def openLatch: Receive = {
          case 'openLatch ⇒ bridgeLatch.countDown()
        }
        override protected def receive = openLatch orElse publishMessage orElse super.receive
      }).start()
      val clientBridge = actorOf(new ZeroMqBridge(context, clientConfig.name) with ClientBridge with SubscriberBridge {
        override def preStart {
          super.preStart
          self ! 'openLatch
        }
        protected def openLatch: Receive = {
          case 'openLatch ⇒ bridgeLatch.countDown()
        }
        override protected def receive = openLatch orElse subscribeToEvents orElse super.receive
      }).start()

      val eventLatch = new StandardLatch
      val client = actorOf(new Actor {
        protected def receive = {
          case 'subscribe ⇒ {
            clientBridge ! Subscribe("the-topic")
          }
          case ApplicationEvent('the_integr_publish, JNothing) ⇒ {
            eventLatch.open()
          }
        }
      }).start()
      given("a client actor subscribes to a topic")
      assert(bridgeLatch.await(2, TimeUnit.SECONDS))
      client ! 'subscribe
      when("when the server actor publishes to that topic")
      assert(subscribeLatch.tryAwait(2, TimeUnit.SECONDS))
      serverBridge ! Publish("the-topic", ApplicationEvent('the_integr_publish))
      then("the client actor should receive the event")
      assert(eventLatch.tryAwait(2, TimeUnit.SECONDS))
    }
  }

  feature("A reliable actor client can communicate with the backend") {

    info("As a reliable 0mq actor client")
    info("I want to be able to communicate with the backend")
    info("And I want to be able to publish events")
    info("And I want to be able to subscribe to events")

    scenario("the actor enqueues a message") {
      given("an initialized system")
      val serverConfig = DeviceConfig(context, "integr-rel-server-actor-enqueue-test", "inproc://integr-b-aaa.inproc")
      val clientConfig = serverConfig.copy(name = "integr-rel-client-actor-enqueue-test")
      val serverLatch = new StandardLatch
      val clientLatch = new StandardLatch
      val clientStarted = new StandardLatch
      val serverBroker = ZeroMQ startDevice {
        new BackchatZeroMqDevice(serverConfig) with ServerActorBridge with PingPongResponder {
          val routerAddress = serverConfig.serverAddress
          override def init() {
            super.init()
            serverLatch.open()
          }
        }
      }
      assert(serverLatch.tryAwait(2, TimeUnit.SECONDS))
      val clientBroker = ZeroMQ startDevice {
        new BackchatZeroMqDevice(clientConfig) with ClientActorBridge with ReliableClientBroker {
          availableServers = new AvailableServers((serverConfig.name + "-endpoint") -> AvailableServer(clientConfig.serverAddress, 5.seconds))
          override def init() {
            super.init()
            clientLatch.open()
          }

          override protected def onConnected(endpoint: String) = {
            super.onConnected(endpoint)
            clientStarted.open()
          }
        }
      }
      val serverActorName = serverConfig.name + "-handler"
      val clientName = clientConfig.name + "-handler"
      val latch = new StandardLatch
      val serverstarted = new StandardLatch
      val serverActor = actorOf(new Actor {
        self.id = serverActorName

        override def preStart {
          self ! 'openLatch
        }

        protected def receive = {
          case 'openLatch                                  ⇒ serverstarted.open()
          case ApplicationEvent('enqueued_event, JNothing) ⇒ latch.open()
        }
      }).start()
      val serverBridge = actorOf(new ZeroMqBridge(context, serverConfig.name) with ServerBridge).start()
      assert(clientLatch.tryAwait(2, TimeUnit.SECONDS))
      assert(serverstarted.tryAwait(2, TimeUnit.SECONDS))
      val client = actorOf(new ZeroMqBridge(context, clientConfig.name) with ClientBridge).start()
      when("an actor enqueues a message")
      assert(clientStarted.tryAwait(2, TimeUnit.SECONDS))
      client ! Enqueue(serverActorName, ApplicationEvent('enqueued_event))
      then("the server actor should receive the message")
      assert(latch.tryAwait(2, TimeUnit.SECONDS))
      serverBroker.stop
      clientBroker.stop
      serverActor.stop()
      client.stop()
    }

    scenario("the actor makes a request") {
      given("an initialized system")
      val serverConfig = DeviceConfig(context, "integr-rel-client-actor-reply-test", "inproc://integr-b-ccc.inproc")
      val clientConfig = serverConfig.copy(name = "integr-rel-server-actor-reply-test")
      val serverLatch = new StandardLatch
      val clientLatch = new StandardLatch
      val clientStarted = new StandardLatch
      val serverBroker = ZeroMQ startDevice {
        new BackchatZeroMqDevice(serverConfig) with ServerActorBridge with PingPongResponder {
          val routerAddress = serverConfig.serverAddress
          override def init() {
            super.init()
            serverLatch.open()
          }
        }
      }
      assert(serverLatch.tryAwait(2, TimeUnit.SECONDS))
      val clientBroker = ZeroMQ startDevice {
        new BackchatZeroMqDevice(clientConfig) with ClientActorBridge with ReliableClientBroker {
          availableServers = new AvailableServers((serverConfig.name + "-endpoint") -> AvailableServer(clientConfig.serverAddress, 5.seconds))
          override def init() {
            super.init()
            clientLatch.open()
          }

          override protected def onConnected(endpoint: String) = {
            super.onConnected(endpoint)
            clientStarted.open()
          }
        }
      }
      val serverActorName = serverConfig.name + "-handler"
      val clientName = clientConfig.name + "-handler"
      val latch = new StandardLatch
      val serverStarted = new StandardLatch
      val serverActor = actorOf(new Actor {
        self.id = serverActorName

        override def preStart {
          self ! 'openLatch
        }

        protected def receive = {
          case 'openLatch ⇒ {
            serverStarted.open()
          }
          case ApplicationEvent('a_client_request, JNothing) ⇒ {
            self.sender foreach { _ ! JsonParser.parse(ApplicationEvent('a_server_reply).toJson) }
          }
        }
      }).start()
      val serverBridge = actorOf(new ZeroMqBridge(context, serverConfig.name) with ServerBridge).start()
      assert(clientLatch.tryAwait(2, TimeUnit.SECONDS))
      val clientBridge = actorOf(new ZeroMqBridge(context, clientConfig.name) with ClientBridge).start()
      val replyLatch = new StandardLatch
      val client = actorOf(new Actor {
        protected def receive = {
          case 'request ⇒ {
            val repl = clientBridge !! Request(serverActorName, ApplicationEvent('a_client_request))
            if (repl.map(_.asInstanceOf[ApplicationEvent]).forall(_.action == 'a_server_reply)) replyLatch.open()
          }
        }
      }).start()
      when("an actor makes a request")
      assert(serverStarted.tryAwait(2, TimeUnit.SECONDS))
      assert(clientStarted.tryAwait(2, TimeUnit.SECONDS))
      client ! 'request
      then("the client actor should receive the response")
      assert(replyLatch.tryAwait(2, TimeUnit.SECONDS))
      client.stop()
      serverActor.stop()
      serverBridge.stop()
      clientBridge.stop()
      clientBroker.stop
      serverBroker.stop
    }
    //
    //    scenario("the actor publishes a message") {
    //      given("an initialized system")
    //      when("an client actor publishes a message")
    //      then("the server actor should receive the message")
    //      pending
    //    }

    scenario("the actor subscribes to a topic") {
      ZeroMQ.trace = true
      val serverConfig = DeviceConfig(context, "integr-rel-server-actor-subscribe-test", "inproc://integr-b-eee.inproc")
      val clientConfig = serverConfig.copy(name = "integr-rel-client-actor-subscribe-test")
      val serverLatch = new StandardLatch
      val clientLatch = new StandardLatch
      val clientStarted = new StandardLatch
      val subscribeLatch = new StandardLatch
      val serverBroker = ZeroMQ startDevice {
        new BackchatZeroMqDevice(serverConfig) with ServerActorBridge with ServerPubSubPublisher with PingPongResponder {

          val routerAddress = serverConfig.serverAddress
          override def init() {
            super.init()
            serverLatch.open()
          }

          override protected def inboundHandler(zmsg: ZMessage) = {
            super.inboundHandler(zmsg)
            if (zmsg.messageType == "pubsub" && zmsg.sender == "subscribe")
              subscribeLatch.open()
          }

        }
      }
      assert(serverLatch.tryAwait(2, TimeUnit.SECONDS))
      val clientBroker = ZeroMQ startDevice {
        new BackchatZeroMqDevice(clientConfig) with ClientActorBridge with ReliableClientBroker with ClientPubSubSubscriber {
          availableServers = new AvailableServers((serverConfig.name + "-endpoint") -> AvailableServer(clientConfig.serverAddress, 5.seconds))
          override def init() {
            super.init()
            clientLatch.open()
          }

          override protected def onConnected(endpoint: String) = {
            super.onConnected(endpoint)
            clientStarted.open()
          }
        }
      }
      val serverActorName = serverConfig.name + "-handler"
      val clientName = clientConfig.name + "-handler"
      assert(clientLatch.tryAwait(2, TimeUnit.SECONDS))
      val bridgeLatch = new CountDownLatch(2)
      val serverBridge = actorOf(new ZeroMqBridge(context, serverConfig.name) with ServerBridge with PublisherBridge {
        override def preStart {
          super.preStart
          self ! 'openLatch
        }
        protected def openLatch: Receive = {
          case 'openLatch ⇒ bridgeLatch.countDown()
        }
        override protected def receive = openLatch orElse publishMessage orElse super.receive
      }).start()
      val clientBridge = actorOf(new ZeroMqBridge(context, clientConfig.name) with ClientBridge with SubscriberBridge {
        override def preStart {
          super.preStart
          self ! 'openLatch
        }
        protected def openLatch: Receive = {
          case 'openLatch ⇒ bridgeLatch.countDown()
        }
        override protected def receive = openLatch orElse subscribeToEvents orElse super.receive
      }).start()

      val eventLatch = new StandardLatch
      val client = actorOf(new Actor {
        protected def receive = {
          case 'subscribe ⇒ {
            clientBridge ! Subscribe("the-topic")
          }
          case ApplicationEvent('the_integr_publish, JNothing) ⇒ {
            eventLatch.open()
          }
        }
      }).start()
      given("a client actor subscribes to a topic")
      assert(bridgeLatch.await(2, TimeUnit.SECONDS))
      assert(clientStarted.tryAwait(2, TimeUnit.SECONDS))
      client ! 'subscribe
      when("when the server actor publishes to that topic")
      assert(subscribeLatch.tryAwait(2, TimeUnit.SECONDS))
      serverBridge ! Publish("the-topic", ApplicationEvent('the_integr_publish))
      then("the client actor should receive the event")
      assert(eventLatch.tryAwait(2, TimeUnit.SECONDS))
      ZeroMQ.trace = false
    }
  }

  feature("A 0mq client can communicate with the backend") {

    info("As a 0mq client")
    info("I want to be able to communicate with the backend")

    scenario("the 0mq client enqueues a message") {
      given("an initialized system")
      val clientConfig = DeviceConfig(context, "integr-blient-enqueue-test", "inproc://integr-aaa.inproc")
      val serverConfig = clientConfig.copy(name = "integr-server-enqueue-test")
      val serverLatch = new StandardLatch
      val clientLatch = new StandardLatch
      val serverBroker = ZeroMQ startDevice {
        new BackchatZeroMqDevice(serverConfig) with ServerActorBridge {
          val routerAddress = serverConfig.serverAddress
          override def init() {
            super.init()
            serverLatch.open()
          }
        }
      }
      assert(serverLatch.tryAwait(2, TimeUnit.SECONDS))
      val clientBroker = ZeroMQ startDevice {
        new BackchatZeroMqDevice(clientConfig) with ClientActorBridge with ClientBroker {
          val outboundAddress = clientConfig.serverAddress
          override def init() {
            super.init()
            clientLatch.open()
          }
        }
      }
      val serverActorName = serverConfig.name + "-handler"
      val clientName = clientConfig.name + "-handler"
      val latch = new StandardLatch
      val serverstarted = new StandardLatch
      val serverActor = actorOf(new Actor {
        self.id = serverActorName

        override def preStart {
          self ! 'openLatch
        }

        protected def receive = {
          case 'openLatch                                  ⇒ serverstarted.open()
          case ApplicationEvent('enqueued_event, JNothing) ⇒ latch.open()
        }
      }).start()
      val serverBridge = actorOf(new ZeroMqBridge(context, serverConfig.name) with ServerBridge).start()
      assert(clientLatch.tryAwait(2, TimeUnit.SECONDS))
      val client = new BackchatZeroMqClient(clientName, context, clientConfig.name)
      when("the 0mq client enqueues an event message")
      assert(serverstarted.tryAwait(2, TimeUnit.SECONDS))
      client.enqueue(serverActorName, ApplicationEvent('enqueued_event))
      then("the server actor should receive the application event")
      assert(latch.tryAwait(2, TimeUnit.SECONDS))
      serverBroker.stop
      clientBroker.stop
      serverActor.stop()
      client.dispose()
    }

    scenario("the 0mq client makes a request") {
      given("a started 0mq client with clientbroker, server broker and started server actor")
      val clientConfig = DeviceConfig(context, "integr-blient-reply-test", "inproc://integr-bcc.inproc")
      val serverConfig = clientConfig.copy(name = "integr-server-reply-test")
      val serverLatch = new StandardLatch
      val clientLatch = new StandardLatch
      val serverBroker = ZeroMQ startDevice {
        new BackchatZeroMqDevice(serverConfig) with ServerActorBridge {
          val routerAddress = serverConfig.serverAddress
          override def init() {
            super.init()
            serverLatch.open()
          }
        }
      }
      assert(serverLatch.tryAwait(2, TimeUnit.SECONDS))
      val clientBroker = ZeroMQ startDevice {
        new BackchatZeroMqDevice(clientConfig) with ClientActorBridge with ClientBroker {
          val outboundAddress = clientConfig.serverAddress
          override def init() {
            super.init()
            clientLatch.open()
          }
        }
      }
      val serverActorName = serverConfig.name + "-handler"
      val clientName = clientConfig.name + "-handler"
      val latch = new StandardLatch
      val serverStarted = new StandardLatch
      val serverActor = actorOf(new Actor {
        self.id = serverActorName

        override def preStart {
          self ! 'openLatch
        }

        protected def receive = {
          case 'openLatch ⇒ {
            serverStarted.open()
          }
          case ApplicationEvent('a_client_request, JNothing) ⇒ {
            self.sender foreach { _ ! JsonParser.parse(ApplicationEvent('a_server_reply).toJson) }
          }
        }
      }).start()
      val serverBridge = actorOf(new ZeroMqBridge(context, serverConfig.name) with ServerBridge).start()
      clientLatch.tryAwait(2, TimeUnit.SECONDS)
      val client = new BackchatZeroMqClient(clientName, context, clientConfig.name)
      when("the client makes a request")
      assert(serverStarted.tryAwait(2, TimeUnit.SECONDS))
      client.request(serverActorName, ApplicationEvent('a_client_request)) { appEvt ⇒
        assert(appEvt == (ApplicationEvent('a_server_reply)))
        latch.open()
      }
      then("the 0mq client should receive the reply")
      assert(latch.tryAwait(2, TimeUnit.SECONDS))
      serverBroker.stop
      clientBroker.stop
      serverActor.stop()
      client.dispose
    }
    // A plain zeromq client doesn't do subscribing
    //
    //    scenario("the client actor subscribes to pubsub topic") {
    //      given("a started client actor subscribed to a pubsub topid")
    //      when("the server publishes a message")
    //      then("the message should get wrapped in the server zeromq bridge")
    //      and("the message should pass through the server broker")
    //      and("the message should arrive in the client broker")
    //      and("the message should get unwrapped in the client zeromq bridge")
    //      and("the event should arrive in the client actor subscriber")
    //      pending
    //    }
    //
    //    scenario("the 0mq client publishes a pubsub topic") {
    //      given("a started client actor with clientbroker, server broker and started server actor")
    //      when("the 0mq client publishes a message")
    //      then("the server actor should receive the event")
    //      pending
    //    }
  }

}