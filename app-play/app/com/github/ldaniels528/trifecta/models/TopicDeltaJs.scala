package com.github.ldaniels528.trifecta.models

import play.api.libs.json.Json

/**
  * Topic Delta model
  *  @author lawrence.daniels@gmail.com
  */
case class TopicDeltaJs(topic: String, partition: Int, startOffset: Long, endOffset: Long, messages: Long)

/**
  * Topic Delta Companion Object
  * @author lawrence.daniels@gmail.com
  */
object TopicDeltaJs {

  implicit val TopicDeltaReads = Json.reads[TopicDeltaJs]

  implicit val TopicDeltaWrites = Json.writes[TopicDeltaJs]

}