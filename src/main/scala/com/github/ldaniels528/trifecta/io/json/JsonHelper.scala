package com.github.ldaniels528.trifecta.io.json

import net.liftweb.json.JsonAST.JValue
import net.liftweb.json._
import org.apache.avro.generic.GenericRecord

import scala.util.{Failure, Success, Try}

/**
  * JSON Helper Utility
  * @author lawrence.daniels@gmail.com
  */
object JsonHelper {
  implicit val formats = DefaultFormats

  def isJson(jsString: String): Boolean = Try(parse(jsString)).isSuccess

  /**
    * Re-formats the given JSON string as a "pretty" version of the JSON string
    * @param jsonString the given JSON string
    * @return a "pretty" version of the JSON string
    */
  def makePretty(jsonString: String): String = {
    Try(toJson(jsonString)) match {
      case Success(js) => prettyRender(js)
      case Failure(e) => jsonString
    }
  }

  /**
    * Transforms the given JSON string into the specified type
    * @param jsonString the given JSON string (e.g. { "symbol":"AAPL", "price":"115.44" })
    * @param manifest   the implicit [[Manifest]]
    * @tparam T the specified type
    * @return an instance of the specified type
    */
  def transform[T](jsonString: String)(implicit manifest: Manifest[T]): T = parse(jsonString).extract[T]

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

  def toJson[T](results: Seq[T]): JValue = Extraction.decompose(results)

  def toJsonString(bean: AnyRef): String = compactRender(Extraction.decompose(bean))

  def compressJson(jsString: String): String = compactRender(parse(jsString))

}
