package com.mojolly.backchat
package zeromq

import akka.actor._
import net.liftweb.json._
import java.util.Locale.ENGLISH

object Messages {

  sealed trait ZeroMqMessage
  sealed trait SystemMessage extends ZeroMqMessage {
    def toZMessage: ZMessage
  }
  sealed abstract class SysMessage(val name: Symbol) extends SystemMessage {
    def toZMessage = ZMessage("", newCcId, "system", "", "", name.name.toUpperCase(ENGLISH))
  }
  case object Ping extends SysMessage('ping)
  case object Pong extends SysMessage('pong)
  case object Ready extends SysMessage('ready)
  case object Hello extends SystemMessage {
    def toZMessage = ZMessage("", newCcId, "system", "HELLO", "", "")
  }
  case class RegisterService(name: String, id: String) extends SystemMessage {
    def toZMessage = ZMessage("", newCcId, "system", "REGISTER", name.toLowerCase(ENGLISH), id)
  }
  case class UnregisterService(name: String, id: String) extends SystemMessage {
    def toZMessage = ZMessage("", newCcId, "system", "UNREGISTER", name.toLowerCase(ENGLISH), id)
  }
  case class Error(ccid: Uuid, message: String) extends SystemMessage {
    def toZMessage = ZMessage("", newCcId, "system", "ERROR", "", message)
  }

  sealed trait RequestMessage extends ZeroMqMessage {
    def toZMessage: ZMessage
  }
  sealed abstract class ReqMessage(val ccid: Uuid, val target: String, val event: ApplicationEvent) extends RequestMessage
  case class Request(override val target: String, override val event: ApplicationEvent, override val ccid: Uuid = (new Uuid), sender: String = "") extends ReqMessage(ccid, target, event) {
    def toZMessage = {
      ZMessage("", ccid.toString, "requestreply", sender, target, event.toJson)
    }
  }
  object Enqueue {
    def apply(pm: ProtocolMessage): Enqueue = {
      Enqueue(pm.target, ApplicationEvent(pm.payload), pm.ccId)
    }
  }
  case class Enqueue(override val target: String, override val event: ApplicationEvent, override val ccid: Uuid = (new Uuid)) extends ReqMessage(ccid, target, event) {
    def toZMessage = {
      ZMessage("", ccid.toString, "fireforget", "", target, event.toJson)
    }
  }

  case class Subscribe(topic: String) extends RequestMessage {
    def toZMessage = {
      ZMessage(topic, "", newCcId, "pubsub", "subscribe", topic, "")
    }
  }

  case class Unsubscribe(topic: String) extends RequestMessage {
    def toZMessage = {
      ZMessage(topic, "", newCcId, "pubsub", "unsubscribe", topic, "")
    }
  }

  case object SubscribeAll extends RequestMessage {
    def toZMessage = {
      ZMessage("", "", newCcId, "pubsub", "subscribe", "", "")
    }
  }

  case object UnsubscribeAll extends RequestMessage {
    def toZMessage = {
      ZMessage("", "", newCcId, "pubsub", "unsubscribe", "", "")
    }
  }

  sealed trait ResponseMessage extends ZeroMqMessage
  sealed abstract class RespMessage[EvtType](val event: EvtType) extends ResponseMessage
  case class Response(override val event: JValue) extends RespMessage(event)
  case class Publish(topic: String, override val event: ApplicationEvent) extends RespMessage(event)

}