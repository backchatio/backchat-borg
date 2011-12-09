package backchat
package borg
package telepathy
package tests

import org.zeromq.ZMQ
import net.liftweb.json.JsonAST.{JArray, JString}
import org.multiverse.api.latches.StandardLatch
import java.util.concurrent.TimeUnit
import akka.zeromq.Frame
import BorgMessage.MessageType
import telepathy.Messages._
import akka.actor._
import mojolly.testing.MojollySpecification


class ClientSpec extends MojollySpecification { def is =
  "A telepathic client should" ^
    "when responding to messages" ^
      "handle an enqueue message" ! context.handlesEnqueue ^
      "handle a request message" ! context.handlesRequest ^
      "publish messages to a pubsub server" ! pending ^
      "subscribe to all pubsub messages" ! pending ^
      "unsubscribe from all pubsub messages" ! pending ^
      "subscribe to specific topics" ! pending ^
      "unsubscribe from specific topics" ! pending ^
    "when providing reliability" ^
      "expect a hug when the tell was received by the server" ! pending ^
      "expect a hug when the ask was received by the server" ! pending ^
      "expect a hug when the shout message was received by the server" ! pending ^
      "expect a hug when the listen message was received by the server" ! pending ^
      "expect a hug when the deafen message was received by the server" ! pending ^
      "reschedule a tell message when no hug received within the time limt" ! pending ^
      "reschedule an ask message when no hug received within the time limt" ! pending ^
      "reschedule a shout message when no hug received within the time limt" ! pending ^
      "reschedule a listen message when no hug received within the time limt" ! pending ^
      "reschedule a deafen message when no hug received within the time limt" ! pending ^
    end
  
  def context = new ClientSpecContext
  
  class ClientSpecContext extends ZeroMqContext {
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
  }
  
  
}