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

trait Telepath extends Actor with Logging {

  self.dispatcher = telepathyDispatcher
  self.faultHandler = OneForOneStrategy(List(classOf[Throwable]), 5, 3000)

  lazy val context = ZeroMQ.newContext()

  def socketListener: Option[ActorRef] = self.some
  implicit def osa2oa(opt: Option[ScalaActorRef]): Option[ActorRef] = opt map (_.asInstanceOf[ActorRef])

  protected def newSocket(socketType: SocketType.Value, options: SocketOption*) = {
    val socketTimeout = options find (_.isInstanceOf[Timeout]) map (_.asInstanceOf[Timeout].value) getOrElse 100L
    val deserializer = options find (_.isInstanceOf[MessageDeserializer]) map (_.asInstanceOf[MessageDeserializer].value) getOrElse new ZMQMessageDeserializer
    val listener: Option[ActorRef] = options find (_.isInstanceOf[SocketListener]) map (_.asInstanceOf[SocketListener].value) orElse Some(self)
    val timeo = akka.util.Duration(socketTimeout, TimeUnit.MILLISECONDS)
    val realOptions = options filterNot {
      case _: Timeout | _: MessageDeserializer | _: SocketListener ⇒ true
      case _ ⇒ false
    }
    val params = SocketParameters(
      context,
      socketType,
      listener,
      deserializer = deserializer,
      pollTimeoutDuration = timeo,
      options = realOptions)
    ZeroMQ.newSocket(params, self.some, self.dispatcher)
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

