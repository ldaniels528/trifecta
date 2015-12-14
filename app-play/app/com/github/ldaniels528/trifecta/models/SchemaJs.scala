package com.github.ldaniels528.trifecta.models

import play.api.libs.json.Json

/**
  * Schema JSON model
  * @author lawrence.daniels@gmail.com
  */
case class SchemaJs(topic: String, name: String, schemaString: String, error: Option[String] = None)

/**
  * Schema JSON Companion Object
  * @author lawrence.daniels@gmail.com
  */
object SchemaJs {

  implicit val SchemaReads = Json.reads[SchemaJs]

  implicit val SchemaWrites = Json.writes[SchemaJs]

}
