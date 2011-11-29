package backchat
package borg
package hive
package telepathy

import akka.actor._
import borg.BorgMessage.MessageType
import akka.zeromq.ZMQMessage
import scalaz._
import Scalaz._

object Messages extends Logging {

  sealed class InvalidMessageException(borgMessage: BorgMessage) extends BorgException("Couldn't parse message: %s".format(borgMessage))
  sealed trait HiveMessage extends BorgMessageWrapper
  sealed trait HiveRequest extends HiveMessage
  sealed trait HiveControlRequest extends HiveRequest
  
  sealed abstract class ControlRequest(val name: Symbol) extends HiveControlRequest {
    val unwrapped = BorgMessage(BorgMessage.MessageType.System, "", ApplicationEvent(name))
  }
  case object Ping extends ControlRequest('ping)
  case object Ready extends ControlRequest('ready)
  case object Hello extends ControlRequest('hello)
  
  case class Tell(target: String, payload: ApplicationEvent, ccid: Uuid = newUuid) extends HiveRequest {
    def unwrapped = BorgMessage(BorgMessage.MessageType.FireForget, target, payload)
  }
  object Ask {
    def apply(target: String,  payload: ApplicationEvent): Ask = {
      Ask(target, "sender", payload)
    }
  }
  case class Ask(target: String, sender: String, payload: ApplicationEvent, ccid: Uuid = newUuid) extends HiveRequest {
    def unwrapped = BorgMessage(BorgMessage.MessageType.RequestReply, target, payload, Some(sender), ccid)
    def respond(payload: ApplicationEvent) = Reply(sender, payload, ccid = ccid)
  }

  sealed trait HiveResponse extends HiveMessage
  sealed trait HiveControlResponse extends HiveResponse

  sealed abstract class ControlResponse(val name: Symbol) extends HiveControlResponse {
    val unwrapped = BorgMessage(MessageType.System, "", ApplicationEvent(name))
  }
  case object Pong extends ControlResponse('pong)
  case class Reply(target: String, payload: ApplicationEvent, ccid: Uuid = newUuid()) extends HiveResponse {
    def unwrapped = BorgMessage(MessageType.RequestReply, target, payload, ccid = ccid)
  } 
  
  def apply(bytes: Seq[Byte]): BorgMessageWrapper = Messages(BorgMessage(bytes))
  def apply(msg: BorgMessage): BorgMessageWrapper = {
    logger debug "Converting message: %s".format(msg)
    msg match {
      case BorgMessage(MessageType.System, _, ApplicationEvent('pong, _), _, _) => Pong
      case BorgMessage(MessageType.FireForget, target, data, _, null) => Tell(target, data)
      case BorgMessage(MessageType.FireForget, target, data, _, ccid) => Tell(target, data, ccid)
      case BorgMessage(MessageType.RequestReply, target, data, Some(sender), null) => Ask(target, sender, data)
      case BorgMessage(MessageType.RequestReply, target, data, Some(sender), ccid) => Ask(target, sender, data, ccid)
      case BorgMessage(MessageType.RequestReply, target, data, None, null) => Reply(target, data)
      case BorgMessage(MessageType.RequestReply, target, data, None, ccid) => Reply(target, data, ccid)
      case m => throw new InvalidMessageException(m)
    }
  }
  
//  def unapply(msg: ZMQMessage) = BorgMessage(msg.frames.last.payload) match {
//    case BorgMessage(MessageType.System, _, ApplicationEvent('pong, _), _, _) => Pong.some
//    case BorgMessage(MessageType.FireForget, target, data, _, null) => Tell(target, data).some
//    case BorgMessage(MessageType.FireForget, target, data, _, ccid) => Tell(target, data, ccid).some
//    case BorgMessage(MessageType.RequestReply, target, data, Some(sender), null) => Ask(target, sender, data).some
//    case BorgMessage(MessageType.RequestReply, target, data, Some(sender), ccid) => Ask(target, sender, data, ccid).some
//    case BorgMessage(MessageType.RequestReply, target, data, None, null) => Reply(target, data).some
//    case BorgMessage(MessageType.RequestReply, target, data, None, ccid) => Reply(target, data, ccid).some
//    case m => None
//  }

}