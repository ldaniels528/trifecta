package com.github.ldaniels528.trifecta.ui.models

import play.api.libs.json.{Json, Reads, Writes}

/**
  * Consumer Offsets
  * @author lawrence.daniels@gmail.com
  */
case class ConsumerOffsetsJs(consumerId: String, topic: String, partition: Int, offset: Long, lastModifiedTime: Option[Long])

/**
  * Consumer Offsets Singleton
  * @author lawrence.daniels@gmail.com
  */
object ConsumerOffsetsJs {

  implicit val ConsumerOffsetsReads: Reads[ConsumerOffsetsJs] = Json.reads[ConsumerOffsetsJs]

  implicit val ConsumerOffsetsWrites: Writes[ConsumerOffsetsJs] = Json.writes[ConsumerOffsetsJs]

}
