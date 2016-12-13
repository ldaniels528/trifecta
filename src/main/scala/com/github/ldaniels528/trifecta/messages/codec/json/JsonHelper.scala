package com.github.ldaniels528.trifecta.messages.codec.json

import net.liftweb.json._
import org.apache.avro.generic.GenericRecord

import scala.util.Try

/**
  * JSON Helper Utility
  * @author lawrence.daniels@gmail.com
  */
object JsonHelper {
  implicit val formats = DefaultFormats

  /**
    * Indicates whether the given string is as valid JSON expression
    * @param jsString the given string
    * @return true, if the given string is as valid JSON expression
    */
  def isJson(jsString: String): Boolean = Try(parse(jsString)).isSuccess

  /**
    * Converts the given collection of values into a JSON value
    * @param results the given collection of values
    * @return the resultant [[JValue]]
    */
  def parseSeq[T](results: Seq[T]): JValue = Extraction.decompose(results)

  def renderJson(jsString: String, pretty: Boolean): String = {
    renderJson(parse(jsString), pretty)
  }

  def renderJson(jValue: JValue, pretty: Boolean): String = {
    if (pretty) prettyRender(jValue) else compactRender(jValue)
  }

  def renderJson(values: Seq[Map[String, Any]], pretty: Boolean): String = {
    import net.liftweb.json.Extraction._
    renderJson(decompose(values), pretty)
  }

  /**
    * Converts the given string into a JSON value
    * @param jsonString the given JSON string
    * @return the resultant [[JValue]]
    */
  def transform(jsonString: String): JValue = parse(jsonString)

  /**
    * Converts the given record into a JSON value
    * @param record the given [[GenericRecord]]
    * @return the resultant [[JValue]]
    */
  def transform(record: GenericRecord): JValue = parse(record.toString)

  /**
    * Converts the data object into a JSON value
    * @param bean the given data object
    * @return the resultant [[JValue]]
    */
  def transformFrom(bean: AnyRef): JValue = Extraction.decompose(bean)

  /**
    * Transforms the given JSON string into the specified type
    * @param jsonString the given JSON string (e.g. { "symbol":"AAPL", "price":"115.44" })
    * @param manifest   the implicit [[Manifest]]
    * @tparam T the specified type
    * @return an instance of the specified type
    */
  def transformTo[T](jsonString: String)(implicit manifest: Manifest[T]): T = parse(jsonString).extract[T]

}