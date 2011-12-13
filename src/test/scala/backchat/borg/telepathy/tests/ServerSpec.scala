package backchat
package borg
package telepathy
package tests

import util.Random
import mojolly.testing.{AkkaSpecification}
import akka.actor._
import Actor._
import akka.testkit._
import Messages._
import mojolly.io.FreePort
import akka.zeromq.{Bind, Send, Frame, ZMQMessage}
import scalaz.Scalaz._
import org.specs2.execute.Result
import net.liftweb.json._
import java.util.concurrent.{CountDownLatch, TimeUnit}

class ServerSpec extends AkkaSpecification { def is =
  "A Server should" ^
    "respond with pong when receiving a ping" ! specify.respondsWithPong ^
    "tracks active pubsub client sessions" ! pending ^ // on first subscription
    "tracks active reliable client sessions" ! specify.tracksReliableClientSessions ^ bt ^ // CanHazHugz
    "when receiving a tell message" ^
      "route to the correct handler" ! specify.routesToCorrectHandler ^
      "do nothing for reliable false" ! specify.doesNothingForUnreliable ^
      "send hug for reliable true" ! specify.sendsHugForReliableClient ^ bt ^
    "when receiving an ask message" ^
      "reply with the response" ! specify.sendsReply ^
    "when receiving a shout message" ^
      "publish the message to the active subscriptions" ! specify.publishesToSubscribers ^
    "when receiving a listen message" ^
      "add a remote listener to the active remote subscriptions" ! specify.addsSubscription ^
      "add a local listener to the active local subscriptions" ! specify.addsLocalSubscription ^
    "when receiving a deafen message" ^
      "remove a remote listener from the active remote subscriptions" ! specify.removesSubscription ^
      "remove a local listener from the active local subscriptions" ! specify.removesLocalSubscription ^
  end
  
  def controlMessages(req: RequestContext) = {
  }

  trait ZMQServerContext extends TestKit {

    val clientId = {
      val newId = Array.empty[Byte]
      Random.nextBytes(newId)
      newId
    }

    val port = FreePort.randomFreePort()
    val address = TelepathAddress("127.0.0.1", port)
    val addressUri = address.address
    lazy val serverConfig = ServerConfig(address, testActor.some)
    def mkMessage(msg: BorgMessageWrapper) = {
      ZMQMessage(Seq(Frame(clientId), Frame(msg.toBytes)))
    }
  }
  
  trait RequestContext extends ZMQServerContext {
    def routesToCorrectHandler: Result
    def doesNothingForUnreliable: Result
    def sendsHugForReliableClient: Result
  }

  def specify = new ServerContext

  class ServerContext extends ZMQServerContext {

    def respondsWithPong = {
      val latch = TestLatch(2)
      val expected = Send(Seq(Frame(clientId), Frame(Pong.toBytes)))
      val socket = actorOf(new Actor {
        def receive = {
          case Bind(`addressUri`) => latch.countDown()
          case `expected` => latch.countDown()
        }
      }).start()
      val server = TestActorRef(new Server(serverConfig.copy(socket = socket.some))).start()
      server ! mkMessage(Ping)
      latch.await(2 seconds) must beTrue 
    }
    
    def tracksReliableClientSessions = {
      val server = TestActorRef(new Server(serverConfig)).start()
      server ! mkMessage(CanHazHugz)
      server.underlyingActor.activeClients must be_==(Vector(ClientSession(clientId))).eventually
    }

    def routesToCorrectHandler = {
      val l = new CountDownLatch(1)
      val target = "the-target"
      actorOf(new Actor {
        self.id = target
        protected def receive = {
          case ApplicationEvent('pingping, JNothing) => l.countDown
        }
      }).start()

      val server = TestActorRef(new Server(serverConfig)).start()
      server ! mkMessage(CanHazHugz)
      server ! mkMessage(Tell(target, ApplicationEvent('pingping)))
      l.await(2, TimeUnit.SECONDS) must beTrue
    }

    def doesNothingForUnreliable = {
      val socketLatch = new CountDownLatch(2)
      val target = "the-target-2"
      val msg = Tell(target, ApplicationEvent('pingping))
      val expected = Hug(msg.ccid)
      val socket = actorOf(new Actor {
        def receive = {
          case Bind(`addressUri`) => socketLatch.countDown
          case `expected` => socketLatch.countDown
        }
      }).start()
      val server = TestActorRef(new Server(serverConfig.copy(socket = socket.some))).start()

      server ! mkMessage(msg)
      socketLatch.await(2, TimeUnit.SECONDS) must beFalse
    }

    def sendsHugForReliableClient = {
      val socketLatch = new CountDownLatch(2)
      val target = "the-target-3"
      val msg = Tell(target, ApplicationEvent('pingping))
      val expected = Hug(msg.ccid)
      val socket = actorOf(new Actor {
        def receive = {
          case Bind(`addressUri`) => socketLatch.countDown
          case `expected` => socketLatch.countDown
        }
      }).start()
      val server = TestActorRef(new Server(serverConfig.copy(socket = socket.some))).start()
      server ! mkMessage(CanHazHugz)
      sleep -> 100.millis
      server ! mkMessage(msg)
      socketLatch.await(2, TimeUnit.SECONDS) must beTrue
    }

    def sendsReply = {
      val socketLatch = new CountDownLatch(3)
      val target = "the-reply-target"
      val sender = "the-sender"
      val req = Ask(target, sender, ApplicationEvent('pingping))
      val hug = Hug(req.ccid)
      val expected = mkMessage(Reply(sender, ApplicationEvent('pongpong), req.ccid)).frames

      actorOf(new Actor {
        self.id = target
        protected def receive = {
          case ApplicationEvent('pingping, JNothing) => self reply ApplicationEvent('pongpong)
        }
      }).start()
      val socket = actorOf(new Actor {
        def receive = {
          case Bind(`addressUri`) => socketLatch.countDown
          case `hug` => socketLatch.countDown
          case Send(`expected`) => socketLatch.countDown
        }
      }).start()

      val server = TestActorRef(new Server(serverConfig.copy(socket = socket.some))).start()
      server ! mkMessage(CanHazHugz)
      server ! mkMessage(req)
      socketLatch.await(2, TimeUnit.SECONDS) must beTrue
    }

    def addsSubscription = {
      val socketLatch = new CountDownLatch(2)
      val topic = "the-add-topic"
      val req = Listen(topic)
      val hug = Hug(req.ccid)
      val socket = actorOf(new Actor {
        def receive = {
          case Bind(`addressUri`) => socketLatch.countDown
          case `hug` => socketLatch.countDown
        }
      }).start()
      val remSubscriptions = TestActorRef[Subscriptions.RemoteSubscriptions].start()
      val server = TestActorRef(new Server(serverConfig.copy(socket = socket.some, remoteSubscriptions = remSubscriptions.some))).start()
      server ! mkMessage(CanHazHugz)
      server ! mkMessage(req)
      remSubscriptions.underlyingActor.topicSubscriptions.size must be_==(1).eventually
    }
    
    def addsLocalSubscription = {
      val topic = "the-add-topic-2"
      val req = Listen(topic)
      val remSubscriptions = TestActorRef[Subscriptions.LocalSubscriptions].start()
      val server = TestActorRef(new Server(serverConfig.copy(localSubscriptions = remSubscriptions.some))).start()
      server ! req
      remSubscriptions.underlyingActor.topicSubscriptions.size must be_==(1).eventually
    }

    def removesSubscription =  {
      val socketLatch = new CountDownLatch(2)
      val topic = "the-add-topic"
      val req = Listen(topic)
      val hug = Hug(req.ccid)
      val socket = actorOf(new Actor {
        def receive = {
          case Bind(`addressUri`) => socketLatch.countDown
          case `hug` => socketLatch.countDown
        }
      }).start()
      val remSubscriptions = TestActorRef[Subscriptions.RemoteSubscriptions].start()
      val server = TestActorRef(new Server(serverConfig.copy(socket = socket.some, remoteSubscriptions = remSubscriptions.some))).start()
      server ! mkMessage(CanHazHugz)
      server ! mkMessage(req)
      remSubscriptions.underlyingActor.topicSubscriptions.size must be_==(1).eventually and {
        server ! mkMessage(Deafen(topic))
        remSubscriptions.underlyingActor.topicSubscriptions.size must be_==(0).eventually
      }
    }

    def removesLocalSubscription = {
      val topic = "the-add-topic-2"
      val req = Listen(topic)
      val remSubscriptions = TestActorRef[Subscriptions.LocalSubscriptions].start()
      val server = TestActorRef(new Server(serverConfig.copy(localSubscriptions = remSubscriptions.some))).start()
      server ! req
      remSubscriptions.underlyingActor.topicSubscriptions.size must be_==(1).eventually and {
        server ! Deafen(topic)
        remSubscriptions.underlyingActor.topicSubscriptions.size must be_==(0).eventually
      }
    }

    def publishesToSubscribers = pending


  }
}