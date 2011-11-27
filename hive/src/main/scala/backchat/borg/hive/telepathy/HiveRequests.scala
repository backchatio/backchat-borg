package backchat
package borg
package hive
package telepathy

object HiveRequests {

  sealed trait HiveRequest extends BorgMessageWrapper
  sealed trait HiveControlRequest extends HiveRequest
  
  sealed abstract class ControlRequest(val name: Symbol) extends HiveControlRequest {
    val unwrapped = BorgMessage(BorgMessage.MessageType.System, "", ApplicationEvent(name))
  }
  case object Ping extends ControlRequest('ping)
  case object Ready extends ControlRequest('ready)
  case object Hello extends ControlRequest('hello)
  
  case class Tell(target: String, payload: ApplicationEvent) extends HiveRequest {
    def unwrapped = BorgMessage(BorgMessage.MessageType.FireForget, target, payload)
  }
  case class Request(target: String, sender: String, payload: ApplicationEvent) extends HiveRequest {
    def unwrapped = BorgMessage(BorgMessage.MessageType.FireForget, target, payload, Some(sender))
  }

}
