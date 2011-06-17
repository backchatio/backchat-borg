package com.mojolly.backchat
package zeromq

import akka.actor._
import Actor._
import collection.immutable.Queue
import org.joda.time.DateTime

case class AddToSentQueue(message: ProtocolMessage)
case class ProcessedSuccessfully(message: ProtocolMessage)
case class RetryingMessage(ccid: Uuid)
case class SentMessage(message: ProtocolMessage, retries: Int = 0) {
  def incrementRetries = SentMessage(message, retries + 1)
}
case class FailedMessage(message: ProtocolMessage, failedAt: DateTime)

class DurableMessages(maxRetriesBeforeFail: Int = 5) extends Actor {

  protected var toSend = Map[Uuid, SentMessage]()
  protected var failed = Queue[FailedMessage]()

  protected def receive = {
    case AddToSentQueue(m) ⇒ {
      toSend += m.ccId -> SentMessage(m)
      failed = failed.filterNot(_.message.ccId == m.ccId)
    }
    case ProcessedSuccessfully(m) ⇒ {
      toSend -= m.ccId
      failed = failed.filterNot(_.message.ccId == m.ccId)
    }
    case RetryingMessage(ccid) ⇒ {
      toSend.get(ccid) foreach { sm ⇒
        if (sm.retries >= maxRetriesBeforeFail) {
          failed = failed enqueue FailedMessage(sm.message, DateTime.now)
        } else {
          toSend += ccid -> sm.incrementRetries
        }
      }
    }
  }
}