package backchat
package borg
package samples

import org.zeromq.ZMQ
import org.multiverse.api.latches.StandardLatch
import java.util.concurrent.TimeUnit
import akka.actor.Actor._
import akka.actor.Actor
import net.liftweb.json._
import backchat.borg.zeromq._
import mojolly.queue.ApplicationEvent


object ClientTestServer extends Logging {
  val context = ZMQ.context(1)
  
  class TestHandler(name: String) extends Actor {
    self.id = name


    override def preStart() {
      logger.info("TestHandler [%s] started." format self.id)
    }

    protected def receive = {
      case ApplicationEvent('pong, JNothing) => {
        logger.info("Received event")
      }
      case ApplicationEvent('a_request, JString("a param")) => {
        logger.info("Received request")
        self reply ApplicationEvent('the_response, JString("reply content")).toJValue
      }
    }
  }
  
  def main(args: Array[String])  {

    val connectionLatch = new StandardLatch()
    val serverConfig = DeviceConfig(context, "client-test-server", "tcp://127.0.0.1:13333")

    ZeroMQ startDevice (
      new BackchatZeroMqDevice(serverConfig) with ServerActorBridge {
        val routerAddress = serverConfig.serverAddress

        override def init() = {
          super.init()
          connectionLatch.open()
        }
      }
    )
    if(connectionLatch.tryAwait(1, TimeUnit.SECONDS)) {
      val serverBridge = actorOf(new ZeroMqBridge(context, serverConfig.name) with ServerBridge).start()
      actorOf(new TestHandler("the-target")).start()
      logger.info("Client test server started.")
    } else {
      logger.info("Couldn't start the server.")
      sys.exit(1)
    }
  }
}