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

trait Telepath extends Actor with Logging {

  self.dispatcher = telepathyDispatcher
  self.faultHandler = OneForOneStrategy(List(classOf[Throwable]), 5, 3000)

  lazy val context = ZeroMQ.newContext()

  def socketListener:Option[ActorRef] = Some(self)
  
  protected def newSocket(socketType: SocketType.Value, options: SocketOption*) = {
    val socketTimeout = options find (_.isInstanceOf[Timeout]) map (_.asInstanceOf[Timeout].value) getOrElse 100L
    val deserializer = options find (_.isInstanceOf[MessageDeserializer]) map (_.asInstanceOf[MessageDeserializer].value) getOrElse new ZMQMessageDeserializer
    val timeo = akka.util.Duration(socketTimeout, TimeUnit.MILLISECONDS)
    val realOptions = options.filterNot(o => o.isInstanceOf[Timeout] || o.isInstanceOf[MessageDeserializer])
    val params = SocketParameters(
      context,
      socketType,
      socketListener,
      deserializer = deserializer,
      pollTimeoutDuration = timeo,
      options = realOptions)
    ZeroMQ.newSocket(params, Some(self), self.dispatcher)
  }
}

case class TelepathAddress(host: String, port: Int, protocol: String = "tcp") {
  def address = "%s://%s:%d" format (protocol, host, port)
}


