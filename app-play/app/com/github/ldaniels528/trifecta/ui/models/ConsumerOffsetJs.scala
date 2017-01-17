package com.github.ldaniels528.trifecta.ui.models

import play.api.libs.json.{Json, Reads, Writes}

case class ConsumerOffsetJs(topic: String,
                            partition: Int,
                            offset: Long,
                            topicStartOffset: Option[Long],
                            topicEndOffset: Option[Long],
                            messages: Option[Long],
                            lastModifiedTime: Option[Long])

object ConsumerOffsetJs {

  implicit val ConsumerOffsetReads: Reads[ConsumerOffsetJs] = Json.reads[ConsumerOffsetJs]
  implicit val ConsumerOffsetWrites: Writes[ConsumerOffsetJs] = Json.writes[ConsumerOffsetJs]

}
