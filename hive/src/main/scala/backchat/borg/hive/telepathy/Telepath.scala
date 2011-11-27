package backchat
package borg
package hive
package telepathy

import scalaz._
import Scalaz._
import akka.actor._
import java.util.concurrent.TimeUnit
import akka.zeromq._
import akka.config.Supervision._

class BorgZMessageDeserializer extends Deserializer[BorgMessage] {
  def apply(frames: Seq[Frame]) = BorgMessage(frames.last.payload)
}

trait Telepath extends Actor with Logging {

  self.dispatcher = telepathyDispatcher
  self.faultHandler = OneForOneStrategy(List(classOf[Throwable]), 5, 3000)

  lazy val context = ZeroMQ.newContext()

  protected def newSocket(socketType: SocketType.Value, timeout: Duration = 100.millis) = {
    val timeo = akka.util.Duration(timeout.getMillis, TimeUnit.MILLISECONDS)
    val params = SocketParameters(context, socketType, Some(self), deserializer, timeo)
    ZeroMQ.newSocket(params, Some(self), self.dispatcher)
  }

  lazy val deserializer = new BorgZMessageDeserializer

}

case class TelepathAddress(host: String, port: Int, protocol: String = "tcp") {
  def address = "%s://%s:%d" format (protocol, host, port)
}


