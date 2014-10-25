package com.ldaniels528.trifecta.support.json

import com.mongodb.casbah.Imports.{DBObject => Q, _}
import net.liftweb.json.JsonAST.JValue
import net.liftweb.json._

/**
 * Trifecta JSON Parsing Utility
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
object TxJsonUtil {
  implicit val formats = DefaultFormats

  def toJson(jsonString: String): JValue = parse(jsonString)

  def toJson(result: Q): JValue = toJson(result.toString)

  def toDocument(js: JValue): Q = {
    js.values match {
      case m: Map[String, Any] => convertToMDB(m).asInstanceOf[Q]
      case x => throw new IllegalArgumentException(s"$x (${Option(x).map(_.getClass.getName)})")
    }
  }

  private def convertToMDB[T](input: T): Any = {
    input match {
      case m: Map[String, Any] =>
        m.foldLeft(Q()) { case (result, (key, value)) =>
          result ++ Q(key -> convertToMDB(value))
        }
      case x => x
    }
  }

}
