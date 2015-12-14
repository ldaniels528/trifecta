package com.github.ldaniels528.trifecta.models

import play.api.libs.json.Json

/**
  * Topic Summary JSON model
  * @author lawrence.daniels@gmail.com
  */
case class TopicSummaryJs(topic: String, partitions: Seq[TopicPartitionJs], totalMessages: Long)

/**
  * Topic Summary JSON Companion Object
  * @author lawrence.daniels@gmail.com
  */
object TopicSummaryJs {

  implicit val TopicSummaryReads = Json.reads[TopicSummaryJs]

  implicit val TopicSummaryWrites = Json.writes[TopicSummaryJs]

}
