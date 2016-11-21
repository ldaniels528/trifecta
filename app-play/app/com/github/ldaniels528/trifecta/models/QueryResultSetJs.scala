package com.github.ldaniels528.trifecta.models

import play.api.libs.json.{JsValue, Json}

/**
  * Query Result Set JSON model
  * @author lawrence.daniels@gmail.com
  */
case class QueryResultSetJs(topic: String, columns: Seq[String], rows: Seq[JsValue])

/**
  * Query Result Set JSON Companion Object
  * @author lawrence.daniels@gmail.com
  */
object QueryResultSetJs {

  implicit val QueryResultSetReads = Json.reads[QueryResultSetJs]

  implicit val QueryResultSetWrites = Json.writes[QueryResultSetJs]

}
