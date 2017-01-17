package com.github.ldaniels528.trifecta.modules.etl.io.device

import com.github.ldaniels528.commons.helpers.OptionHelper._
import com.github.ldaniels528.trifecta.modules.etl.io.Scope
import com.github.ldaniels528.trifecta.modules.etl.io.record._
import play.api.libs.json.JsObject

/**
  * Represents a Data Set
  * @author lawrence.daniels@gmail.com
  */
case class DataSet(data: Seq[(String, Option[Any])]) {

  def convertToBinary(record: Record)(implicit scope: Scope): Array[Byte] = record match {
    case rec: BinarySupport => rec.toBytes(this)
    case rec: JsonSupport => rec.toJson(this).toString().getBytes()
    case rec: TextSupport => rec.toText(this).getBytes()
    case rec => throw new UnsupportedRecordTypeException(rec)
  }

  def convertToJson(record: Record)(implicit scope: Scope): JsObject = record match {
    case rec: BinarySupport => JsonSupport.parse(new String(rec.toBytes(this)))
    case rec: JsonSupport => rec.toJson(this)
    case rec: TextSupport => JsonSupport.parse(rec.toText(this))
    case rec => throw new UnsupportedRecordTypeException(rec)
  }

  def convertToText(record: Record)(implicit scope: Scope): String = record match {
    case rec: TextSupport => rec.toText(this)
    case rec: JsonSupport => rec.toJson(this).toString()
    case rec: BinarySupport => new String(rec.toBytes(this))
    case rec => throw new UnsupportedRecordTypeException(rec)
  }

  def values(fields: Seq[Field]) = {
    fields map (field => field.name -> toMap.get(field.name) ?? getDefaultValue(field))
  }

  private def getDefaultValue(field: Field) = {
    field.defaultValue flatMap {
      case value if value.contains("{{") && value.contains("}}") =>
        val start = value.indexOf("{{")
        val end = value.indexOf("}}")
        val newValue = (if (end > start && start >= 0) new StringBuilder(value).replace(start, end + 2, "") else value).toString.trim
        if (newValue.nonEmpty) Some(newValue) else None
      case value => Option(value)
    }
  }

  lazy val toMap = Map(data flatMap { case (name, value) => value.map(name -> _) }: _*)

}


