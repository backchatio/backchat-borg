package backchat
package zeromq


import net.liftweb.json._
import java.util.Locale.ENGLISH

object CoreExtensions {

  private val underscoreStepOne = "([A-Z]+)([A-Z][a-z])".r
  private val underscoreStep2 = "([a-z\\d])([A-Z])".r
  private val underscoreReplace = "$1_$2"


  class BackchatJValue(jvalue: JValue) {
    def toJson = asJson(jvalue)
    def toPrettyJson = asJson(jvalue, true)
    def camelizeKeys = rewriteJsonAST(jvalue, true)
    def snakizeKeys = rewriteJsonAST(jvalue, false)

    private def asJson(value: JValue, pretty: Boolean = false) = {
      val doc = render(value)
      if (pretty) Printer.pretty(doc) else Printer.compact(doc)
    }

    private def rewriteJsonAST(json: JValue, camelize: Boolean): JValue = {
      json transform {
        case JField(nm, x) if !nm.startsWith("_") ⇒ JField(if (camelize) nm.camelize else nm.snakeize, x)
        case x                                    ⇒ x
      }
    }
  }

  class BackchatString(s: String) {
    def camelize = {
      val lst = s.split("_").toList
      (lst.head :: lst.tail.map(s ⇒ s.take(1).toUpperCase + s.substring(1))).mkString("")
    }

    def snakeize = {
      underscoreStep2.replaceAllIn(
        underscoreStepOne.replaceAllIn(
          s, underscoreReplace), underscoreReplace).replace('-', '_').toLowerCase(ENGLISH)
    }

    def isBlank = s == null || s.trim.isEmpty
    def isNotBlank = !isBlank
    def toOption = if (isBlank) None else Some(s)
  }

}