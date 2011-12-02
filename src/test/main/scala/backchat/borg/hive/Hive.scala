package backchat
package borg
package hive

import BorgMessage.MessageType
import net.liftweb.json._

object Hive {

  sealed trait HiveMessage
  sealed trait HiveProtocolMessage extends HiveMessage with BorgMessageWrapper

  case class ICanHaz(subtree: String, ccid: String) extends HiveProtocolMessage {
    def unwrapped = BorgMessage(MessageType.System, "", ApplicationEvent('i_can_haz, JString(subtree)))
  }

  case class KVSync(key: String, value: String)

  case class KThxBai()
}