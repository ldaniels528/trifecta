package com.github.ldaniels528.trifecta.modules.etl.io.record

import java.util.Date

import com.github.ldaniels528.trifecta.modules.etl.io.Scope
import com.github.ldaniels528.trifecta.modules.etl.io.device.DataSet
import com.github.ldaniels528.trifecta.modules.etl.io.record.DataTypes._
import com.github.ldaniels528.trifecta.modules.etl.io.record.JsonSupport._
import com.github.ldaniels528.commons.helpers.OptionHelper.Risky._
import com.github.ldaniels528.commons.helpers.OptionHelper._
import org.slf4j.LoggerFactory
import play.api.libs.json._

/**
  * Represents JSON-representation support capability for a record
  * @author lawrence.daniels@gmail.com
  */
trait JsonSupport {
  self: Record =>

  protected val logger = LoggerFactory.getLogger(getClass)

  def fromJson(jsonString: String)(implicit scope: Scope) = {
    DataSet(Json.parse(jsonString) match {
      case jsObject: JsObject => toProperties(jsObject)
      case js =>
        throw new IllegalArgumentException(s"Unhandled JSON value '$js' (${js.getClass.getSimpleName})")
    })
  }

  def toJson(dataSet: DataSet)(implicit scope: Scope): JsObject = {
    val jsValues = fields zip dataSet.values(fields) map { case (f, (_, v)) => f.name -> f.convertToJson(v) }
    jsValues.foldLeft(Json.obj()) { case (js, (k, v)) => js ++ Json.obj(k -> v) }
  }

  protected def findFieldInPath(originalFields: Seq[Field], property: String) = {
    val path = property.split("[.]")
    val firstName = path.headOption orDie "Path is empty"
    val firstField = fields.find(_.name == firstName) orDie s"$firstName of path $property not found"

    path.tail.foldLeft(firstField) { case (field, name) =>
      field.elements.find(_.name == name) orDie s"$name of path $property not found"
    }
  }

  protected def toProperties(jsObject: JsObject, prefix: Option[String] = None): List[(String, Option[Any])] = {
    def fullName(name: String) = prefix.map(s => s"$s.$name") getOrElse name

    jsObject.value.foldLeft[List[(String, Option[Any])]](Nil) { case (list, (name, js)) =>
      val result: List[(String, Option[Any])] = js match {
        case JsBoolean(value) => List(fullName(name) -> value)
        case JsNull => List(fullName(name) -> None)
        case JsNumber(value) => List(fullName(name) -> value.toDouble)
        case JsString(value) => List(fullName(name) -> value)
        case jo: JsObject => toProperties(jo, prefix = fullName(name))
        case unknown =>
          throw new IllegalArgumentException(s"Could not convert property '$name' type '$unknown' to a Scala value")
      }
      result ::: list
    }
  }

}

/**
  * Json Support Companion Object
  * @author lawrence.daniels@gmail.com
  */
object JsonSupport {

  def parse(jsonString: String): JsObject = {
    Json.parse(jsonString) match {
      case jo: JsObject => jo
      case js =>
        throw new IllegalArgumentException(s"JSON object expected - $js")
    }
  }

  /**
    * Json Support Enrichment
    * @param field the given [[Field field]]
    */
  implicit class JsonSupportEnrichment(val field: Field) extends AnyVal {

    def convertToJson(aValue: Option[Any])(implicit scope: Scope): JsValue = {
      aValue map { value =>
        field.`type` match {
          case BOOLEAN => JsBoolean(value == "true")
          case DATE => value match {
            case date: Date => JsNumber(date.getTime)
            case number => JsNumber(number.toString.toLong)
          }
          case DOUBLE | FLOAT | INT | LONG => JsNumber(value.toString.toDouble)
          case STRING => JsString(value.toString)
          case unknown =>
            throw new IllegalArgumentException(s"Could not convert type '$unknown' to JSON")
        }
      } getOrElse JsNull
    }
  }

}

