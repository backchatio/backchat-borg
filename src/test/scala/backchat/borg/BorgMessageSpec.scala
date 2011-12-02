package backchat.borg

import mojolly.testing.MojollySpecification
import akka.actor._
import net.liftweb.json.JsonAST.{JString, JArray}
import com.google.protobuf.ByteString

class BorgMessageSpec extends MojollySpecification { def is =
  "A BorgMessage should" ^
    "serialize into the correct format" ! context.serializeCorrectlyToProtobuf ^
    "deserialize into the correct message" ! context.deserializeCorrectlyFromProtobuf ^
    "serialize into the correct format with event data" ! context.serializeCorrectlyToProtobuf2 ^
    "deserialize into the correct message with event data" ! context.deserializeCorrectlyFromProtobuf2 ^ end

  def context = new specContext
  class specContext {
    val ccid = new Uuid
    val msg = BorgMessage(
      BorgMessage.MessageType.RequestReply,
      "named_service",
      ApplicationEvent('testing),
      Some("sender"),
      ccid)

    val ccid2 = new Uuid
    val msg2 = BorgMessage(
      BorgMessage.MessageType.FireForget,
      "named_service2",
      ApplicationEvent('testing2, JArray(JString("data") :: Nil)),
      Some("sender2"),
      ccid2)

    def serializeCorrectlyToProtobuf = {
      val actual = msg.toProtobuf
      Seq(actual.getTarget must_== "named_service",
          actual.getMessageType must_== Protos.BorgMessage.MessageType.REQUEST_REPLY,
          actual.getCcid must_== ccid.toString,
          actual.getSender must_== "sender",
          actual.getPayload.getAction must_== "testing",
          actual.getPayload.getData.isEmpty must beTrue) reduce (_ and _)
    }
    
    def deserializeCorrectlyFromProtobuf = {
      BorgMessage(msg.toProtobuf) must_== msg
    }

    def serializeCorrectlyToProtobuf2 = {
      val actual = msg2.toProtobuf
      Seq(actual.getTarget must_== "named_service2",
          actual.getMessageType must_== Protos.BorgMessage.MessageType.FIRE_FORGET,
          actual.getCcid must_== ccid2.toString,
          actual.getSender must_== "sender2",
          actual.getPayload.getAction must_== "testing2",
          actual.getPayload.getData.isEmpty must beFalse,
          actual.getPayload.getData must_== ByteString.copyFromUtf8("[\"data\"]")) reduce (_ and _)
    }

    def deserializeCorrectlyFromProtobuf2 = {
      BorgMessage(msg2.toProtobuf) must_== msg2
    }
  }
}