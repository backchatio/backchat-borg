package backchat
package borg
package zeromq
package tests

import org.scalatest.WordSpec
import org.scalatest.matchers.MustMatchers
import ProtocolMessage.InvalidProtocolMessageException
import akka.actor.Uuid

class ProtocolMessageSpec extends WordSpec with MustMatchers {

  "A protocol message" should {

    "Read a valid protocol message" in {
      val ccid = newCcId
      val msg = ZMessage("address_1", "address_2", "", ccid, "message_type", "", "the-target", "the message body")
      ProtocolMessage(msg) must equal(ProtocolMessage(new Uuid(ccid), "message_type", None, "the-target", "the message body"))
    }

    "Throw an InvalidProtocolMessageException for missing messageType" in {
      val msg = ZMessage("the message body")
      val thrown = evaluating { ProtocolMessage(msg) } must produce[InvalidProtocolMessageException]
      thrown.getMessage must equal("missing message type")
    }
  }
}