package com.github.ldaniels528.trifecta.ui.models

import play.api.libs.json.{Json, Reads, Writes}

/**
  * Topic Delta model
  * @example {{{ {"topic":"test","partition":2,"startOffset":1,"endOffset":0,"messages":0,"totalMessages":0} }}}
  * @author lawrence.daniels@gmail.com
  */
case class TopicDeltaJs(topic: String,
                        partition: Int,
                        startOffset: Long,
                        endOffset: Long,
                        messages: Long,
                        totalMessages: Long)

/**
  * Topic Delta Companion Object
  * @author lawrence.daniels@gmail.com
  */
object TopicDeltaJs {

  implicit val TopicDeltaReads: Reads[TopicDeltaJs] = Json.reads[TopicDeltaJs]

  implicit val TopicDeltaWrites: Writes[TopicDeltaJs] = Json.writes[TopicDeltaJs]

}