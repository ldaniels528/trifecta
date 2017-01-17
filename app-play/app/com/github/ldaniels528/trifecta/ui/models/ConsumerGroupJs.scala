package com.github.ldaniels528.trifecta.ui.models

import com.github.ldaniels528.trifecta.io.kafka.KafkaZkUtils.{ConsumerOwner, ConsumerThread}
import play.api.libs.json.{Json, Reads, Writes}

/**
  * Consumer Group Model
  * @author lawrence.daniels@gmail.com
  */
case class ConsumerGroupJs(consumerId: String,
                           offsets: Seq[ConsumerOffsetJs],
                           owners: Seq[ConsumerOwner],
                           threads: Seq[ConsumerThread])

/**
  * Consumer Group Singleton
  * @author lawrence.daniels@gmail.com
  */
object ConsumerGroupJs {

  implicit val ConsumerOwnerReads: Reads[ConsumerOwner] = Json.reads[ConsumerOwner]
  implicit val ConsumerOwnerWrites: Writes[ConsumerOwner] = Json.writes[ConsumerOwner]

  implicit val ConsumerThreadReads: Reads[ConsumerThread] = Json.reads[ConsumerThread]
  implicit val ConsumerThreadWrites: Writes[ConsumerThread] = Json.writes[ConsumerThread]

  implicit val ConsumerGroupReads: Reads[ConsumerGroupJs] = Json.reads[ConsumerGroupJs]
  implicit val ConsumerGroupWrites: Writes[ConsumerGroupJs] = Json.writes[ConsumerGroupJs]

}