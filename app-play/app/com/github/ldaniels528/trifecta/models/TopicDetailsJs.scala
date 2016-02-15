package com.github.ldaniels528.trifecta.models

import com.github.ldaniels528.trifecta.io.kafka.KafkaMicroConsumer.TopicDetails
import com.github.ldaniels528.trifecta.models.BrokerJs._
import com.github.ldaniels528.commons.helpers.OptionHelper.Risky._
import play.api.libs.json.Json

/**
  * Topic Details JSON model
  * @author lawrence.daniels@gmail.com
  */
case class TopicDetailsJs(topic: String,
                          partition: Int,
                          startOffset: Option[Long] = None,
                          endOffset: Option[Long] = None,
                          messages: Option[Long] = None,
                          leader: Option[BrokerJs] = None,
                          replicas: Seq[BrokerJs] = Nil,
                          isr: Seq[BrokerJs] = Nil,
                          sizeInBytes: Option[Int] = None)

/**
  * Topic Details JSON Companion Object
  * @author lawrence.daniels@gmail.com
  */
object TopicDetailsJs {

  implicit val TopicDetailsReads = Json.reads[TopicDetailsJs]

  implicit val TopicDetailsWrites = Json.writes[TopicDetailsJs]

  /**
    * Topic Details Conversion
    * @param details the given [[TopicDetails topic details]]
    */
  implicit class TopicDetailsConversion(val details: TopicDetails) extends AnyVal {

    def asJson = TopicDetailsJs(
      topic = details.topic,
      partition = details.partitionId,
      leader = details.leader.map(_.asJson),
      replicas = details.replicas.map(_.asJson),
      isr = details.isr.map(_.asJson),
      sizeInBytes = details.sizeInBytes
    )
  }

}
