package backchat
package borg

import akka.actor._
import scalaz._
import Scalaz._
import net.liftweb.json._
import mojolly.Enum
import borg.Protos.BorgPayload
import com.google.protobuf.{ Message, ByteString }

trait MessageSerialization {

  type ProtoBufMessage <: Message

  def toJValue = Extraction.decompose(this)
  def toProtobuf: ProtoBufMessage

  def toJson = toJValue.toString
  def toBytes = toProtobuf.toByteArray
  def toByteString = toProtobuf.toByteString

}

trait BorgMessageWrapper extends MessageSerialization {
  type ProtoBufMessage = Protos.BorgMessage

  def unwrapped: BorgMessage

  override def toJValue = unwrapped.toJValue

  def toProtobuf = unwrapped.toProtobuf
}

case class BorgMessage(messageType: BorgMessage.MessageType.EnumVal, target: String, payload: ApplicationEvent, sender: Option[String] = None, ccid: Uuid = new Uuid) extends MessageSerialization {

  type ProtoBufMessage = Protos.BorgMessage

  def toProtobuf: Protos.BorgMessage = {
    val builder = Protos.BorgMessage.newBuilder()
    builder.setMessageType(messageType.pbType).setTarget(target).setPayload(pbPayload).setCcid(ccid.toString)
    sender foreach builder.setSender
    builder.build()
  }

  private def pbPayload = {
    val b = BorgPayload.newBuilder.setAction(payload.action.name)
    payload.data match {
      case JNothing  ⇒
      case d: JValue ⇒ b setData (ByteString copyFrom d.toJson.getBytes(Utf8))
    }
    b.build()
  }
}

object BorgMessage {

  object MessageType extends Enum {
    sealed trait EnumVal extends Value {
      def pbType: Protos.BorgMessage.MessageType
    }

    val System = new EnumVal {
      val name = "system"
      val pbType = Protos.BorgMessage.MessageType.SYSTEM
    }

    val FireForget = new EnumVal {
      val name = "fireforget"
      val pbType = Protos.BorgMessage.MessageType.FIRE_FORGET
    }

    val RequestReply = new EnumVal {
      val name = "requestreply"
      val pbType = Protos.BorgMessage.MessageType.REQUEST_REPLY
    }

    val PubSub = new EnumVal {
      val name = "pubsub"
      val pbType = Protos.BorgMessage.MessageType.PUBSUB
    }

    def apply(protocol: Protos.BorgMessage.MessageType) = protocol match {
      case Protos.BorgMessage.MessageType.SYSTEM        ⇒ System
      case Protos.BorgMessage.MessageType.FIRE_FORGET   ⇒ FireForget
      case Protos.BorgMessage.MessageType.REQUEST_REPLY ⇒ RequestReply
      case Protos.BorgMessage.MessageType.PUBSUB        ⇒ PubSub
    }

  }

  def apply(bytes: Seq[Byte]): BorgMessage = BorgMessage(Protos.BorgMessage.parseFrom(bytes.toArray))
  def apply(bytes: ByteString): BorgMessage = BorgMessage(Protos.BorgMessage.parseFrom(bytes))
  def apply(protocol: Protos.BorgMessage): BorgMessage = {
    new BorgMessage(
      MessageType(protocol.getMessageType),
      protocol.getTarget,
      applicationEvent(protocol.getPayload).get,
      protocol.getSender.toOption,
      new Uuid(protocol.getCcid))
  }

  def applicationEvent(protocol: Protos.BorgPayload): Option[ApplicationEvent] = {
    def action = Symbol(protocol.getAction)
    def data: JValue = JsonParser.parse(protocol.getData.toStringUtf8).camelizeKeys
    (protocol.hasAction, protocol.hasData) match {
      case (false, _) ⇒ None
      case (_, false) ⇒ ApplicationEvent(action).some
      case (_, true)  ⇒ ApplicationEvent(action, data).some
    }
  }
}
