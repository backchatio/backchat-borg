package backchat
package borg
package hive
package telepathy

import akka.zeromq._
import akka.actor.ActorRef
import telepathy.HiveRequests.Tell

case class TelepathClientConfig(server: TelepathAddress, listener: Option[ActorRef] = None)

class Client(config: TelepathClientConfig) extends Telepath {

  lazy val socket = newSocket(SocketType.Dealer)

  override def preStart() {
    self ! 'init
  }

  protected def receive = {
    case 'init => {
      socket ! Connect(config.server.address)
      logger debug "Establishing connection to server"
    }
    case m: Tell => {
      logger debug "enqueuing a message to a server"
      socket ! serialize(m)
    }
    case m: Request => {
      logger debug "Sending a request to a server"
      socket ! serialize(m)
    }
    case Connecting => {
      logger debug "Connecting to server"
      config.listener foreach { _ ! 'Connected }
    }
    case Closed => {
      logger debug "Socket connection closed"
    }
  }
  
  private def serialize(msg: BorgMessageWrapper) = Send(Seq(Frame(msg.unwrapped.toBytes)))
}