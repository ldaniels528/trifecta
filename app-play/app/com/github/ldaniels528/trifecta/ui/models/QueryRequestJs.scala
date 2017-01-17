package com.github.ldaniels528.trifecta.ui.models

import play.api.libs.json.{Json, Reads, Writes}

/**
  * Query JSON model
  * @author lawrence.daniels@gmail.com
  */
case class QueryRequestJs(queryString: String)

/**
  * Query JSON Companion Object
  * @author lawrence.daniels@gmail.com
  */
object QueryRequestJs {

  implicit val QueryRequestReads: Reads[QueryRequestJs] = Json.reads[QueryRequestJs]

  implicit val QueryRequestWrites: Writes[QueryRequestJs] = Json.writes[QueryRequestJs]

}
