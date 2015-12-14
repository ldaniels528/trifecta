package com.github.ldaniels528.trifecta.models

import play.api.libs.json.Json

/**
  * Topic Delta With Totals JSON model
  * @author lawrence.daniels@gmail.com
  */
case class TopicDeltaWithTotalsJs(topic: String, partition: Int, startOffset: Long, endOffset: Long, messages: Long, totalMessages: Long)

/**
  * Topic Delta With Totals JSON Companion Object
  * @author lawrence.daniels@gmail.com
  */
object TopicDeltaWithTotalsJs {

  implicit val TopicDeltaWithTotalsReads = Json.reads[TopicDeltaWithTotalsJs]

  implicit val TopicDeltaWithTotalsWrites = Json.writes[TopicDeltaWithTotalsJs]

}
