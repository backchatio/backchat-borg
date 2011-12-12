package backchat
package borg
package telepathy

import akka.actor._
import borg.BorgMessage.MessageType
import akka.zeromq.ZMQMessage
import scalaz._
import Scalaz._
import net.liftweb.json._
import java.io.File

object Messages extends Logging {

  sealed class InvalidMessageException(borgMessage: BorgMessage) extends BorgException("Couldn't parse message: %s".format(borgMessage))
  sealed trait HiveMessage extends BorgMessageWrapper
  sealed trait HiveRequest extends HiveMessage {
    def target: String
    def ccid: Uuid
  }
  sealed trait HiveControlRequest extends HiveMessage

  sealed abstract class ControlRequest(val name: Symbol) extends HiveControlRequest {
    val unwrapped = BorgMessage(BorgMessage.MessageType.System, "", ApplicationEvent(name))
    def ccid = unwrapped.ccid
  }
  case object Ping extends ControlRequest('ping)
  case object CanHazHugz extends ControlRequest('can_haz_hugz)

  case class Tell(target: String, payload: ApplicationEvent, ccid: Uuid = newUuid) extends HiveRequest {
    def unwrapped = BorgMessage(BorgMessage.MessageType.FireForget, target, payload, ccid = ccid)
  }
  case class Shout(target: String, payload: ApplicationEvent, ccid: Uuid = newUuid) extends HiveRequest {
    def unwrapped = BorgMessage(BorgMessage.MessageType.PubSub, target, payload, Some("publish"), ccid)
  }

  case class Listen(target: String, ccid: Uuid = newUuid) extends HiveRequest {
    def unwrapped = BorgMessage(BorgMessage.MessageType.PubSub, target, ApplicationEvent('listen), ccid = ccid)
  }

  case class Deafen(target: String, ccid: Uuid = newUuid) extends HiveRequest {
    def unwrapped = BorgMessage(BorgMessage.MessageType.PubSub, target, ApplicationEvent('deafen), ccid = ccid)
  }

  object Ask {
    def apply(target: String, payload: ApplicationEvent): Ask = {
      Ask(target, "sender", payload)
    }
  }
  case class Ask(target: String, sender: String, payload: ApplicationEvent, ccid: Uuid = newUuid) extends HiveRequest {
    def unwrapped = BorgMessage(BorgMessage.MessageType.RequestReply, target, payload, Some(sender), ccid)
    def respond(payload: ApplicationEvent) = Reply(sender, payload, ccid = ccid)
    def serviceUnavailable = ServiceUnavailable(sender, target, ccid)
    def error(message: String) = Error(sender, message, ccid)
  }

  sealed trait HiveResponse extends HiveMessage
  sealed trait HiveControlResponse extends HiveMessage

  sealed abstract class ControlResponse(val name: Symbol, val ccid: Uuid = newUuid) extends HiveControlResponse {
    val unwrapped = BorgMessage(MessageType.System, "", ApplicationEvent(name), ccid = ccid)
  }
  case object Pong extends ControlResponse('pong)
  case class Reply(target: String, payload: ApplicationEvent, ccid: Uuid = newUuid()) extends HiveResponse {
    def unwrapped = BorgMessage(MessageType.RequestReply, target, payload, ccid = ccid)
  }
  case class Error(target: String, message: String, ccid: Uuid) extends HiveResponse {
    def unwrapped = BorgMessage(MessageType.RequestReply, target, ApplicationEvent('error, JString(message)), ccid = ccid)
  }
  case class ServiceUnavailable(target: String, service: String, ccid: Uuid) extends HiveResponse {
    def unwrapped = BorgMessage(MessageType.RequestReply, target, ApplicationEvent('service_unavailable, JString(service)), ccid = ccid)
  }
  case class Hug(override val ccid: Uuid) extends ControlResponse('hug, ccid)

  sealed trait InternalMessage
  case object Init extends InternalMessage
  case object HappyGoLucky extends InternalMessage
  case object Paranoid extends InternalMessage
  sealed trait Hugging extends InternalMessage
  case class NoLovin(request: HiveRequest) extends Hugging
  case object Hugged extends Hugging

  def apply(bytes: Seq[Byte]): BorgMessageWrapper = Messages(BorgMessage(bytes))
  def apply(msg: BorgMessage): BorgMessageWrapper = {
    logger debug "Converting message: %s".format(msg)
    msg match {
      case BorgMessage(MessageType.System, _, ApplicationEvent('pong, _), _, _) ⇒ Pong
      case BorgMessage(MessageType.System, _, ApplicationEvent('ping, _), _, _) ⇒ Ping
      case BorgMessage(MessageType.FireForget, target, data, _, null) ⇒ Tell(target, data)
      case BorgMessage(MessageType.FireForget, target, data, _, ccid) ⇒ Tell(target, data, ccid)
      case BorgMessage(MessageType.RequestReply, target, data, Some(sender), null) ⇒ Ask(target, sender, data)
      case BorgMessage(MessageType.RequestReply, target, data, Some(sender), ccid) ⇒ Ask(target, sender, data, ccid)
      case BorgMessage(MessageType.RequestReply, target, data, None, null) ⇒ Reply(target, data)
      case BorgMessage(MessageType.RequestReply, target, data, None, ccid) ⇒ Reply(target, data, ccid)
      case BorgMessage(MessageType.RequestReply, target, ApplicationEvent('error, JString(data)), _, ccid) ⇒ Error(target, data, ccid)
      case BorgMessage(MessageType.RequestReply, target, ApplicationEvent('service_unavailable, JString(data)), None, ccid) ⇒ ServiceUnavailable(target, data, ccid)
      case BorgMessage(MessageType.PubSub, target, data, Some("publish"), null) ⇒ Shout(target, data)
      case BorgMessage(MessageType.PubSub, target, data, Some("publish"), ccid) ⇒ Shout(target, data, ccid)
      case BorgMessage(MessageType.PubSub, target, ApplicationEvent('listen, JNothing), _, null) ⇒ Listen(target)
      case BorgMessage(MessageType.PubSub, target, ApplicationEvent('listen, JNothing), _, ccid) ⇒ Listen(target, ccid)
      case BorgMessage(MessageType.PubSub, target, ApplicationEvent('deafen, JNothing), _, null) ⇒ Deafen(target)
      case BorgMessage(MessageType.PubSub, target, ApplicationEvent('deafen, JNothing), _, ccid) ⇒ Deafen(target, ccid)
      case BorgMessage(MessageType.System, _, ApplicationEvent('hug, _), _, ccid) ⇒ Hug(ccid)
      case BorgMessage(MessageType.System, _, ApplicationEvent('can_haz_hugz, _), _, _) ⇒ CanHazHugz
      case m ⇒ throw new InvalidMessageException(m)
    }
  }

}