package backchat
package borg
package samples

import akka.actor._
import Actor._
import org.zeromq.ZMQ
import org.multiverse.api.latches.StandardLatch
import java.util.concurrent.TimeUnit
import net.liftweb.json._
import backchat.borg.zeromq._

object ReliableActorServer {
  val context = ZMQ.context(1)

  def main(args: Array[String]) {

    val connectionLatch = new StandardLatch()
    val serverConfig = DeviceConfig(context, "reliable-actor-server", "tcp://127.0.0.1:13242")

    ZeroMQ startDevice (new BackchatZeroMqDevice(serverConfig) with ServerActorBridge {
      val routerAddress = serverConfig.serverAddress

      override def init() = {
        super.init()
        connectionLatch.open()
      }
    })
    if (connectionLatch.tryAwait(1, TimeUnit.SECONDS)) {
      val serverBridge = actorOf(new ZeroMqBridge(context, serverConfig.name) with ServerBridge).start()
      actorOf(new Actor {
        self.id = "the-target"
        protected def receive = {
          case ApplicationEvent('pong, JNothing) ⇒ {
            println("Received event")
          }
          case ApplicationEvent('a_request, JString("a param")) ⇒ {
            println("Received request")
            self reply ApplicationEvent('the_response, JString("reply content")).toJValue
          }
        }
      }).start()
      println("Reliable actor server started.")
    } else {
      println("Couldn't start the server.")
      sys.exit(1)
    }
  }
}