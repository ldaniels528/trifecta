package com.github.ldaniels528.trifecta.models

import com.github.ldaniels528.trifecta.TxConfig.TxQuery
import play.api.libs.json.Json

/**
  * Query Details JSON model
  * @author lawrence.daniels@gmail.com
  */
case class QueryDetailsJs(name: String, topic: String, queryString: String, exists: Boolean, lastModified: Long)

/**
  * Query Details JSON Companion Object
  * @author lawrence.daniels@gmail.com
  */
object QueryDetailsJs {

  implicit val QueryDetailReads = Json.reads[QueryDetailsJs]

  implicit val QueryDetailWrites = Json.writes[QueryDetailsJs]

  /**
    * Query Details Conversion 
    * @param txq the given [[TxQuery query]]
    */
  implicit class QueryDetailsConversion(val txq: TxQuery) extends AnyVal {

    def asJson = QueryDetailsJs(
      name = txq.name,
      topic = txq.topic,
      queryString = txq.queryString,
      exists = txq.exists,
      lastModified = txq.lastModified
    )
  }

}
