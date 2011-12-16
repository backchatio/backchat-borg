package backchat
package borg
package telepathy

import scalaz._
import Scalaz._
import akka.actor._
import java.util.concurrent.TimeUnit
import akka.zeromq._
import akka.config.Supervision._
import rl.Uri

case class SocketParams(
              socketType: SocketType.Value,
              timeout: Duration = 100 millis, 
              deserializer: Deserializer = new ZMQMessageDeserializer,
              listener: Option[ActorRef] = None)
trait Telepath extends Actor with Logging {
  
  implicit def duration2akkaduration(dur: Duration) = akka.util.Duration(dur.millis, TimeUnit.MILLISECONDS)

//  self.dispatcher = telepathyDispatcher
  self.faultHandler = OneForOneStrategy(List(classOf[Throwable]), 5, 3000)

  lazy val context = ZeroMQ.newContext()

  def socketListener: Option[ActorRef] = self.some
  implicit def osa2oa(opt: Option[ScalaActorRef]): Option[ActorRef] = opt map (_.asInstanceOf[ActorRef])

  protected def newSocket(params: SocketParams, options: SocketOption*) = {
    val parameters = SocketParameters(
      context,
      params.socketType,
      params.listener orElse Some(self),
      deserializer = params.deserializer,
      pollTimeoutDuration = params.timeout)
    val sock = ZeroMQ.newSocket(parameters, self.some)
    options foreach { sock ! _ }
    sock
  }
}
object TelepathAddress {
  def apply(address: String): TelepathAddress = {
    val uri = Uri(address)
    val auth = uri.authority.get
    TelepathAddress(auth.host.value, auth.port, uri.scheme.scheme)
  }

  def apply(host: String, port: Int): TelepathAddress = {
    TelepathAddress(host, Some(port))
  }

  def apply(host: String, port: Int, protocol: String): TelepathAddress = {
    TelepathAddress(host, Some(port), protocol)
  }
}
case class TelepathAddress(host: String, port: Option[Int], protocol: String = "tcp") {
  def address = "%s://%s%s" format (protocol, host, port some (":%d" format _) none "")
}

