package com.ldaniels528.trifecta.support.json

import com.mongodb.casbah.Imports.{DBObject => Q, _}
import net.liftweb.json.JsonAST.JValue
import net.liftweb.json._
import org.apache.avro.generic.GenericRecord

/**
 * Trifecta JSON Parsing Utility
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
object TxJsonUtil {
  implicit val formats = DefaultFormats

  /**
   * Converts the given record into a JSON value
   * @param record the given [[GenericRecord]]
   * @return the resultant [[JValue]]
   */
  def toJson(record: GenericRecord): JValue = parse(record.toString)

  /**
   * Converts the given string into a JSON value
   * @param jsonString the given JSON string
   * @return the resultant [[JValue]]
   */
  def toJson(jsonString: String): JValue = parse(jsonString)

  /**
   * Converts the given MongoDB document into a JSON value
   * @param result the given  MongoDB document
   * @return the resultant [[JValue]]
   */
  def toJson(result: Q): JValue = toJson(result.toString)

  /**
   * Converts the given JSON value into a MongoDB document
   * @param js the given [[JValue]]
   * @return the resultant MongoDB document
   */
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
