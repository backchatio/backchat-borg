package backchat
package borg

import akka.actor.Uuid

object ProtocolMessage {

  case class InvalidProtocolMessageException(msg: String) extends Exception(msg)

  def apply(zmsg: ZMessage): ProtocolMessage = {
    if (zmsg.messageType.isBlank) throw InvalidProtocolMessageException("missing message type")
    new ProtocolMessage(
      if (zmsg.ccid.isBlank) new Uuid() else new Uuid(zmsg.ccid),
      zmsg.messageType,
      zmsg.sender.toOption,
      zmsg.target,
      zmsg.body)
  }

  def apply(ccId: Uuid, messageType: String, sender: Option[String], target: String, payload: ApplicationEvent): ProtocolMessage = {
    new ProtocolMessage(ccId, "requestreply", sender, target, payload.toJson)
  }

}

/**
 * A Protocol message is an immutable version of a ZMessage and used for the actor communication
 */
case class ProtocolMessage(ccId: Uuid, messageType: String, sender: Option[String], target: String, payload: String) {

  def apply(socket: Socket) = {
    toZMessage(socket)
  }
  def toZMessage: ZMessage = {
    ZMessage(ccId.toString, messageType, sender getOrElse "", target, payload)
  }

}
