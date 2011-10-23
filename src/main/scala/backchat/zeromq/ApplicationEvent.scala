package backchat
package zeromq

import net.liftweb.json.JsonParser
import net.liftweb.json.JsonAST._

class MalformedEventException(json: JValue) extends RuntimeException("Malformed json event:\n%s".format(json.toJson))
sealed trait EventMessage extends NotNull {
  def toJson: String
}


object ApplicationEvent {
  def apply(json: JValue): ApplicationEvent = {
    json.camelizeKeys match {
      case JArray(JString(name) :: data :: _) ⇒ ApplicationEvent(Symbol(name), data)
      case JArray(JString(name) :: _)         ⇒ ApplicationEvent(Symbol(name), JNothing)
      case _                                  ⇒ throw new MalformedEventException(json)
    }
  }

  def apply(json: String): ApplicationEvent = {
    ApplicationEvent(JsonParser.parse(json))
  }

  def apply(action: Symbol): ApplicationEvent = {
    ApplicationEvent(action, JNothing)
  }
}
case class ApplicationEvent(action: Symbol, data: JValue) extends EventMessage {
  def toJson: String = JArray(JString(action.name) :: data :: Nil).toJson
}