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

trait FromZMQMessage[TMessage] {
  def fromZMQMessage(msg: ZMQMessage): TMessage
}

trait ToZMQMessage[TMessage] {
  def toZMQMessage(msg: TMessage): ZMQMessage
}

trait ZMQMessageFormat[TMessage] extends FromZMQMessage[BorgMessage] with ToZMQMessage[BorgMessage]

class BorgZMQMessageSerializer extends ZMQMessageFormat[BorgMessage] with Logging {

  def fromZMQMessage(msg: ZMQMessage) = {
    logger debug "Received [%d] frames".format(msg.frames.size)
    BorgMessage(msg.frames.last.payload)
  }

  def toZMQMessage(msg: BorgMessage) = ZMQMessage(msg.toProtobuf)
}


trait Telepath extends Actor with Logging {

  self.dispatcher = telepathyDispatcher
  self.faultHandler = OneForOneStrategy(List(classOf[Throwable]), 5, 3000)

  lazy val context = ZeroMQ.newContext()

  protected def newSocket(socketType: SocketType.Value, socketTimeout: Duration = 100.millis) = {
    val timeo = akka.util.Duration(socketTimeout.getMillis, TimeUnit.MILLISECONDS)
    val params = SocketParameters(context, socketType, Some(self), pollTimeoutDuration = timeo)
    ZeroMQ.newSocket(params, Some(self), self.dispatcher)
  }

}

case class TelepathAddress(host: String, port: Int, protocol: String = "tcp") {
  def address = "%s://%s:%d" format (protocol, host, port)
}


