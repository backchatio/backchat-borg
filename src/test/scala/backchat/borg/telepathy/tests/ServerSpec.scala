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

class ServerSpec extends AkkaSpecification { def is =
  "A Server should" ^
    "respond with pong when receiving a ping" ! specify.respondsWithPong ^
    "tracks active pubsub client sessions" ! pending ^ // on first subscription
    "tracks active reliable client sessions" ! specify.tracksReliableClientSessions ^ bt ^ // CanHazHugz
    "when receiving a tell message" ^
      controlMessages(null) ^ bt  ^
    "when receiving an ask message" ^
      "reply with the response" ! pending ^
      controlMessages(null) ^ bt  ^
    "when receiving a shout message" ^
      "publish the message to the active subscriptions" ! pending ^
      controlMessages(null) ^ bt  ^
    "when receiving a listen message" ^
      "add the listener to the active subscriptions" ! pending ^
      controlMessages(null) ^ bt  ^
    "when receiving a deafen message" ^
      "remove the listener from the active subscriptions" ! pending ^
      controlMessages(null) ^
  end
  
  def controlMessages(req: RequestContext) = {
    "route to the correct handler" ! pending ^
    "do nothing for reliable false" ! pending ^
    "send hug for reliable true" ! pending
  }

  trait RequestContext

  def specify = new ServerContext
  
  class ServerContext extends RequestContext with TestKit {
    
    val clientId = {
      val newId = Array.empty[Byte]
      Random.nextBytes(newId)
      newId
    }
    
    val port = FreePort.randomFreePort()
    val address = TelepathAddress("127.0.0.1", port)
    val addressUri = address.address
    lazy val serverConfig = ServerConfig(address, testActor.some)
    
    
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

    def mkMessage(msg: BorgMessageWrapper) = {
      ZMQMessage(Seq(Frame(clientId), Frame(msg.toBytes)))
    }
  }
}