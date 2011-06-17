package com.mojolly.backchat.zeromq

import net.liftweb.json._

object CoreExtensions {
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

}