package com.github.ldaniels528.trifecta.ui.models

import play.api.libs.json.{Json, Reads, Writes}

/**
  * Consumer Offsets
  * @author lawrence.daniels@gmail.com
  */
case class ConsumerOffsetUpdateJs(consumerId: String, topic: String, partition: Int, offset: Long, lastModifiedTime: Option[Long])

/**
  * Consumer Offsets Singleton
  * @author lawrence.daniels@gmail.com
  */
object ConsumerOffsetUpdateJs {

  implicit val ConsumerOffsetUpdateReads: Reads[ConsumerOffsetUpdateJs] = Json.reads[ConsumerOffsetUpdateJs]

  implicit val ConsumerOffsetUpdateWrites: Writes[ConsumerOffsetUpdateJs] = Json.writes[ConsumerOffsetUpdateJs]

}
