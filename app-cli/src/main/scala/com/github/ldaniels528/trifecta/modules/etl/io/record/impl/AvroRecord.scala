package com.github.ldaniels528.trifecta.modules.etl.io.record.impl

import com.github.ldaniels528.trifecta.modules.etl.io.Scope
import com.github.ldaniels528.trifecta.modules.etl.io.device.DataSet
import com.github.ldaniels528.trifecta.modules.etl.io.record._
import com.github.ldaniels528.trifecta.io.avro.AvroConversion._
import org.apache.avro.Schema
import play.api.libs.json.{JsArray, JsObject, JsString, Json}

/**
  * Avro Record implementation
  * @author lawrence.daniels@gmail.com
  */
case class AvroRecord(id: String, name: String, namespace: String, fields: Seq[Field])
  extends Record with BinarySupport with JsonSupport with TextSupport {

  private val schema = new Schema.Parser().parse(toSchemaString)

  override def fromBytes(bytes: Array[Byte])(implicit scope: Scope) = {
    fromJson(transcodeAvroBytesToAvroJson(schema, bytes))
  }

  override def toBytes(dataSet: DataSet)(implicit scope: Scope) = {
    transcodeJsonToAvroBytes(toJson(dataSet).toString(), schema)
  }

  override def fromText(jsonString: String)(implicit scope: Scope) = fromJson(jsonString)

  override def toText(dataSet: DataSet)(implicit scope: Scope) = toJson(dataSet).toString()

  /**
    * Generates the Avro Schema
    */
  def toSchemaString: String = {
    Json.obj(
      "type" -> "record",
      "name" -> name,
      "namespace" -> namespace,
      "doc" -> "auto-generated comment",
      "fields" -> JsArray(fields.foldLeft[List[JsObject]](Nil) { (list, field) =>
        Json.obj(
          "name" -> field.name,
          "doc" -> "auto-generated comment") ++ toAvroType(field) :: list
      })) toString()
  }

  /**
    * Generates the Avro type object (e.g. '"type": ["null", "string"]')
    * @param field the given field whose type is being modeled
    * @return the type object
    */
  private def toAvroType(field: Field) = {
    if (field.nullable.contains(true))
      Json.obj("type" -> JsArray(Seq("null", field.`type`.toTypeName) map JsString))
    else
      Json.obj("type" -> field.`type`.toTypeName)
  }

}
