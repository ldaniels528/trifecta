package com.github.ldaniels528.trifecta.util

import play.api.Logger
import play.api.libs.json.{JsObject, _}

/**
  * Json Utils
  * @author lawrence.daniels@gmail.com
  */
object QueryJsonUtils {

  /**
    * JSON Conversions
    * @param mapping the given mapping
    */
  implicit class JsonConversions[T](val mapping: Map[String, T]) extends AnyVal {

    def toJson = JsObject(mapping.toSeq map { case (name, value) => (name, wrap(value)) })

  }

  private def wrap(value: Any): JsValue = {
    value match {
      case a if a == null => JsNull
      case a: JsValue => a
      case b: Boolean => JsBoolean(b)
      case m: Map[_, _] => JsObject(m.toSeq map { case (k, v) => (k.toString, wrap(v)) })
      case n: BigDecimal => JsNumber(n)
      case n: BigInt => JsNumber(n.doubleValue())
      case n: Double => JsNumber(n)
      case n: Int => JsNumber(n)
      case n: Long => JsNumber(n)
      case s: Seq[_] => JsArray(s map wrap)
      case s: String => JsString(s)
      case u =>
        Logger.warn(s"Unrecognized type '$u' (${u.getClass.getName})")
        JsNull
    }
  }

}
