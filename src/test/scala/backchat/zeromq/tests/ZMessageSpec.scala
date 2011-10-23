package backchat
package zeromq
package tests

import org.scalatest.WordSpec
import org.scalatest.matchers.MustMatchers
import akka.actor.Uuid

class ZMessageSpec extends WordSpec with MustMatchers {

  val ccid = newCcId
  val ccuuid = new Uuid(ccid)
  val zmsg = ZMessage("sender-1", "sender-2", "", ccid, "requestreply", "the-sender", "the-target", "the body")

  "A ZMessage" should {

    "return the size of its parts" in {
      val parts = List("hello".getBytes, "world".getBytes, "the".getBytes, "message".getBytes)
      val msg = ZMessage("hello", "world", "the", "message", "is", "this")
      msg.size must equal(6)
    }

    "get the correct part as body" in {
      zmsg.body must equal("the body")
    }

    "get the message type" in {
      zmsg.messageType must equal("requestreply")
    }

    "get the ccid" in {
      zmsg.ccid must equal(ccid)
    }

    "get the sender" in {
      zmsg.sender must equal("the-sender")
    }

    "get the target" in {
      zmsg.target must equal("the-target")
    }

    "get the socket addresses along the way" in {
      zmsg.addresses.map(new String(_, "UTF-8")) must equal(Seq("sender-1", "sender-2"))
    }

    "allow replacing the addresses" in {
      zmsg.addresses = Seq("new-sender-1".getBytes, "new-sender-2".getBytes)
      zmsg.addresses.map(new String(_, "UTF-8")) must equal(Seq("new-sender-1", "new-sender-2"))
    }
  }
}