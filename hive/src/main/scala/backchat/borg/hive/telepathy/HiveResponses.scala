package backchat
package borg
package hive
package telepathy

import backchat.borg.{BorgMessage, BorgMessageWrapper}

object HiveResponses {
  sealed trait HiveResponse extends BorgMessageWrapper
  sealed trait HiveControlResponse extends HiveResponse

  sealed abstract class ControlResponse(val name: Symbol) extends HiveControlResponse {
    val unwrapped = BorgMessage(BorgMessage.MessageType.System, "", ApplicationEvent(name))
  }
  case object Pong extends ControlResponse('pong)

  def apply(bytes: Array[Byte]): HiveResponse = HiveResponses(BorgMessage(bytes))
  def apply(msg: BorgMessage): HiveResponse = msg match {
    case BorgMessage(BorgMessage.MessageType.System, _, ApplicationEvent('pong, _), _, _) => Pong
  }
}