package com.github.ldaniels528.trifecta.models

import play.api.libs.json.Json

/**
  * Query JSON model
  * @author lawrence.daniels@gmail.com
  */
case class QueryJs(name: String, topic: String, queryString: String)

/**
  * Query JSON Companion Object
  * @author lawrence.daniels@gmail.com
  */
object QueryJs {

  implicit val QueryReads = Json.reads[QueryJs]

  implicit val QueryWrites = Json.writes[QueryJs]

}
