package backchat
package borg
package hive
package telepathy
package tests

import org.zeromq.ZMQ
import mojolly.io.FreePort
import org.zeromq.ZMQ.{Context, Poller, Socket}
import collection.mutable.ListBuffer
import net.liftweb.json.JsonAST.{JArray, JString}
import org.multiverse.api.latches.StandardLatch
import java.util.concurrent.TimeUnit
import akka.zeromq.{ZMQMessage, Frame}
import org.specs2.execute.Result
import BorgMessage.MessageType
import telepathy.Messages._
import akka.actor._
import mojolly.testing.{MojollySpecification}
import org.specs2.specification.{Around, After, Step, Fragments}
import util.DynamicVariable



class ClientSpec extends MojollySpecification { def is =
  "A telepathic client should" ^
    "when responding to messages" ^
      "handle an enqueue message" ! context.handlesEnqueue ^
      "handle a request message" ! context.handlesRequest ^ end
  
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