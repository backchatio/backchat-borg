package backchat
package borg
package telepathy
package tests

import org.zeromq.ZMQ
import net.liftweb.json.JsonAST.{JArray, JString}
import java.util.concurrent.TimeUnit
import akka.zeromq.Frame
import BorgMessage.MessageType
import akka.actor._
import net.liftweb.json._
import mojolly.testing.{AkkaSpecification}
import telepathy.Messages._
import org.multiverse.api.latches.StandardLatch
import akka.testkit.{TestLatch, TestActorRef}

class ClientSpec extends AkkaSpecification { def is =
/*
*/
    "A telepathic client should" ^
      "when responding to messages" ^
        "handle an enqueue message" ! context.handlesEnqueue ^
        "handle a request message" ! context.handlesRequest ^
        "publish messages to a pubsub server" ! context.handlesShout ^
        "subscribe to pubsub topics" ! context.handlesListen ^
        "add the subscription to the subscription manager" ! context.addsSubscriptionToManager ^
        "unsubscribe from pubsub topics" ! context.handlesDeafen ^
        "remove subscription from the subscription manager" ! context.removesSubscriptionFromManager ^bt ^
      "when providing reliability" ^
        "expect a hug when the tell was received by the server" ! context.expectsHugForTell ^
        "expect a hug when the ask was received by the server" ! context.expectsHugForAsk ^
        "expect a hug when the shout message was received by the server" ! context.expectsHugForShout ^
        "expect a hug when the listen message was received by the server" ! context.expectsHugForListen ^
        "expect a hug when the deafen message was received by the server" ! context.expectsHugForDeafen ^
        "reschedule a tell message when no hug received within the time limt" ! context.handlesNoHugsForTell ^
        "reschedule an ask message when no hug received within the time limt" ! context.handlesNoHugsForAsk ^
        "reschedule a shout message when no hug received within the time limt" ! context.handlesNoHugsForShout ^
        "reschedule a listen message when no hug received within the time limt" ! context.handlesNoHugsForListen ^
        "reschedule a deafen message when no hug received within the time limt" ! context.handlesNoHugsForDeafen ^
        "send pings to the server if no activity for the specified period" ! context.sendsPings ^
    end
  
  def context = new ClientSpecContext
  
  class ClientSpecContext extends ZeroMqContext {

    val subscriptions = TestActorRef[Subscriptions.LocalSubscriptions].start()

    def handlesEnqueue = this {
      withServer() { server =>
        withClient(server.address) { client =>
          val appEvt = ApplicationEvent('pingping, JArray(JString("the message") :: Nil))
          val latch = new StandardLatch
  
          server onMessage { (frames: Seq[Frame]) =>
            val msg = zmqMessage(frames)
            if (msg.target == "target" && msg.payload == appEvt) {
              latch.open()
            }
          }
  
          client ! Tell("target", appEvt)
          server poll 2.seconds
          latch.tryAwait(2, TimeUnit.SECONDS) must beTrue
        }
      }
    }

    
    def handlesRequest = this {
      withServer() { server =>
        withClient(server.address) { client =>
          val appEvt = ApplicationEvent('pingping, JArray(JString("the message") :: Nil))
          val appEvt2 = ApplicationEvent('pongpong, JArray(JString("the response message") :: Nil))
  
          server onMessage { (frames: Seq[Frame]) =>
            val msg = zmqMessage(frames)
            println("server received: %s" format msg)
            if (msg.target == "target" && msg.payload == appEvt && msg.messageType == MessageType.RequestReply) {
              server.socket.send(frames.head.payload.toArray, ZMQ.SNDMORE)
              server.socket.send(Messages(msg).asInstanceOf[Ask].respond(appEvt2).toBytes, 0)
            }
          }
  
          val req = client ? Ask("target", appEvt)
          server poll 2.seconds
          req.as[ApplicationEvent] must beSome(appEvt2)
        }
      }
    }
    
    def handlesShout = this {
      withServer() { server =>
        withClient(server.address) { client => 
          val appEvt = ApplicationEvent('pingping, JArray(JString("the message") :: Nil))
          val latch = new StandardLatch()
          
          server onMessage { (frames: Seq[Frame]) =>
            val msg = zmqMessage(frames)
            msg match {
              case BorgMessage(MessageType.PubSub, "target", `appEvt`, Some("publish"), _) => {
                latch.open()
              }
              case _ =>
            }
          }
          
          client ! Shout("target", appEvt)
          server poll 2.seconds
          latch.tryAwait(2, TimeUnit.SECONDS) must beTrue
        }
      }
    }
    
    def handlesListen = this {
      withServer() { server =>
        withClient(server.address, Some(subscriptions)) { client =>
          val topic = "the-topic"
          val latch = new StandardLatch()
          server onMessage { (frames: Seq[Frame]) =>
            zmqMessage(frames) match {
              case BorgMessage(MessageType.PubSub, `topic`, ApplicationEvent('listen, JNothing), _, _) =>
                latch.open
              case _ =>
            }
          }
          client ! Listen(topic)
          server poll 2.seconds
          latch.tryAwait(2, TimeUnit.SECONDS) must beTrue
        }
      }
    }

    def addsSubscriptionToManager = this {
      withServer() { server =>
        withClient(server.address, subscriptionManager = Some(subscriptions)) { client =>
          val topic = "the-topic"
          val latch = new StandardLatch()
          server onMessage { (frames: Seq[Frame]) =>
            zmqMessage(frames) match {
              case BorgMessage(MessageType.PubSub, `topic`, ApplicationEvent('listen, JNothing), _, _) =>
                latch.open
              case _ =>
            }
          }
          client ! Listen(topic)
          server poll 2.seconds
          val res = latch.tryAwait(2, TimeUnit.SECONDS) must beTrue
          val subs = subscriptions.underlyingActor.topicSubscriptions
          res and (subs must not beEmpty) and (subs(topic) must_== Set(testActor))
        }
      }
    }
    
    def handlesDeafen = this {
      withServer() { server =>
        withClient(server.address, Some(subscriptions)) { client =>
          val topic = "the-topic"
          val latch = new StandardLatch()
          server onMessage { (frames: Seq[Frame]) =>
            zmqMessage(frames) match {
              case BorgMessage(MessageType.PubSub, `topic`, ApplicationEvent('deafen, JNothing), _, _) =>
                latch.open
              case _ =>
            }
          }
          client ! Listen(topic)
          client ! Deafen(topic)
          2 times { server poll 2.seconds }
          latch.tryAwait(2, TimeUnit.SECONDS) must beTrue
        }
      }
    }

    def removesSubscriptionFromManager = this {
      withServer() { server =>
        withClient(server.address, subscriptionManager = Some(subscriptions)) { client =>
          val topic = "the-topic"
          val latch = new StandardLatch()
          server onMessage { (frames: Seq[Frame]) =>
            zmqMessage(frames) match {
              case BorgMessage(MessageType.PubSub, `topic`, ApplicationEvent('deafen, JNothing), _, _) =>
                latch.open
              case _ =>
            }
          }
          client ! Listen(topic)
          client ! Deafen(topic)
          2 times { server poll 2.seconds }
          val res = latch.tryAwait(2, TimeUnit.SECONDS) must beTrue
          val subs = subscriptions.underlyingActor.topicSubscriptions
          res and (subs must beEmpty)
        }
      }
    }
    
    private def expectsHugFor(msg: HiveRequest) = this {
      withServer() { server => 
        withClient(server.address, subscriptionManager = Some(subscriptions)) { client =>
          val l1 = TestLatch()
          server onMessage { (frames: Seq[Frame]) => 
            Messages(zmqMessage(frames)) match {
              case `msg` => {
                server.socket.send(frames.head.payload.toArray, ZMQ.SNDMORE)
                server.socket.send(Hug(msg.ccid).toBytes, 0)
              }
              case CanHazHugz => l1.countDown()
            }
          }
          client ! Paranoid
          server poll 2.seconds
          l1 await 10.millis
          msg match {
            case _: Ask => client ? msg
            case _ => client ! msg
          }
          val r1 = client.underlyingActor.expectedHugs.get(msg.ccid) must beSome[ExpectedHug].eventually
          server poll 2.seconds
          r1 and (client.underlyingActor.expectedHugs.get(msg.ccid) must beNone.eventually)
        }
      }
    } 
    
    def expectsHugForTell = expectsHugFor(Tell("target", ApplicationEvent('pingping)))
    
    def expectsHugForAsk = expectsHugFor(Ask("target", ApplicationEvent('pingping)))
    
    def expectsHugForShout = expectsHugFor(Shout("topic", ApplicationEvent('pingping)))
    
    def expectsHugForListen = expectsHugFor(Listen("topic"))
    
    def expectsHugForDeafen = expectsHugFor(Deafen("topic"))
    
    def handlesNoHugsForTell = this {
      withServer() { server =>
        withClient(server.address) { client =>
          val topic = "the-topic"
          client ! Paranoid
          client ! Tell(topic, ApplicationEvent('pingping))

          var msg: NoLovin = null
          val expires = 6.seconds.from(DateTime.now)
          while (msg == null && expires >= DateTime.now) {
            receiveOne(0.seconds) match {
              case m: NoLovin => msg = m
              case _ =>
            }
          }
          msg must not beNull
        }
      }
    }
    
    def handlesNoHugsForAsk =  this {
      withServer() { server =>
        withClient(server.address) { client =>
          val topic = "the-topic"
          client ! Paranoid
          val res = (client.ask(Ask(topic, ApplicationEvent('pingping)), 7.seconds.millis)).get
          res must beAnInstanceOf[NoLovin]
        }
      }
    }
    
    def handlesNoHugsForShout =  this {
      withServer() { server =>
        withClient(server.address) { client =>
          val topic = "the-topic"
          client ! Paranoid
          client ! Shout(topic, ApplicationEvent('pingping))

          var msg: NoLovin = null
          val expires = 6.seconds.from(DateTime.now)
          while (msg == null && expires >= DateTime.now) {
            receiveOne(0.seconds) match {
              case m: NoLovin => msg = m
              case _ =>
            }
          }
          msg must not beNull
        }
      }
    }
    
    def handlesNoHugsForListen =  this {
      withServer() { server =>
        withClient(server.address, subscriptionManager = Some(subscriptions)) { client =>
          val topic = "the-topic"
          client ! Paranoid
          client ! Listen(topic)

          var msg: NoLovin = null
          val expires = 7.seconds.from(DateTime.now)
          while (msg == null && expires >= DateTime.now) {
            receiveOne(0.seconds) match {
              case m: NoLovin => msg = m
              case _ =>
            }
          }
          msg must not beNull
        }
      }
    }
    
    def handlesNoHugsForDeafen =  this {
      withServer() { server =>
        withClient(server.address, subscriptionManager = Some(subscriptions)) { client =>
          val topic = "the-topic"
          client ! Paranoid
          client ! Deafen(topic)

          var msg: NoLovin = null
          val expires = 7.seconds.from(DateTime.now)
          while (msg == null && expires >= DateTime.now) {
            receiveOne(0.seconds) match {
              case m: NoLovin => msg = m
              case _ =>
            }
          }
          msg must not beNull
        }
      }
    }

    def sendsPings = pending
  }
  
  
}