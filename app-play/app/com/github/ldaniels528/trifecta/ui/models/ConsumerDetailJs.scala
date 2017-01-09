package com.github.ldaniels528.trifecta.ui.models

import com.github.ldaniels528.trifecta.io.kafka.KafkaMicroConsumer.ConsumerDetailsPM
import com.github.ldaniels528.trifecta.io.kafka.KafkaZkUtils.ConsumerDetails
import com.github.ldaniels528.trifecta.ui.models.ConsumerDetailJs.ConsumerDeltaKey
import play.api.libs.json.{Json, Reads, Writes}

/**
  * Consumer Detail JSON model
  * @author lawrence.daniels@gmail.com
  */
case class ConsumerDetailJs(consumerId: String,
                            version: Option[Int],
                            threadId: Option[String],
                            topic: String,
                            partition: Int,
                            offset: Long,
                            topicOffset: Option[Long],
                            lastModified: Option[Long],
                            lastModifiedISO: Option[String],
                            messagesLeft: Option[Long],
                            rate: Option[Double]) {

  def getKey = ConsumerDeltaKey(consumerId, topic, partition)

}

/**
  * Consumer Detail JSON Companion Object
  * @author lawrence.daniels@gmail.com
  */
object ConsumerDetailJs {

  implicit class ConsumerDetailsExtensions(val c: ConsumerDetails) extends AnyVal  {

    def toJson(topicOffset: Option[Long], messagesLeft: Option[Long]): ConsumerDetailJs = {
      ConsumerDetailJs(
        consumerId = c.consumerId,
        version = c.version,
        threadId = c.threadId,
        topic = c.topic,
        partition = c.partition,
        offset = c.offset,
        topicOffset = topicOffset,
        lastModified = c.lastModified,
        lastModifiedISO = c.lastModifiedISO,
        messagesLeft = messagesLeft,
        rate = None)
    }
  }

  implicit class ConsumerDetailsPMExtensions(val c: ConsumerDetailsPM) extends AnyVal  {

    def toJson(topicOffset: Option[Long], messagesLeft: Option[Long]): ConsumerDetailJs = {
      ConsumerDetailJs(
        consumerId = c.topologyName,
        version = None,
        threadId = None,
        topic = c.topic,
        partition = c.partition,
        offset = c.offset,
        topicOffset = topicOffset,
        lastModified = c.lastModified,
        lastModifiedISO = c.lastModifiedISO,
        messagesLeft = messagesLeft,
        rate = None)
    }
  }

  implicit val ConsumerDetailReads: Reads[ConsumerDetailJs] = Json.reads[ConsumerDetailJs]

  implicit val ConsumerDetailWrites: Writes[ConsumerDetailJs] = Json.writes[ConsumerDetailJs]

  case class ConsumerDeltaKey(consumerId: String, topic: String, partition: Int)

}
