package com.github.ldaniels528.trifecta.models

import com.github.ldaniels528.trifecta.models.ConsumerDetailJs.ConsumerDeltaKey
import play.api.libs.json.Json

/**
  * Consumer Detail JSON model
  * @author lawrence.daniels@gmail.com
  */
case class ConsumerDetailJs(consumerId: String, topic: String, partition: Int, offset: Long, topicOffset: Option[Long], lastModified: Option[Long], messagesLeft: Option[Long], rate: Option[Double]) {

  def getKey = ConsumerDeltaKey(consumerId, topic, partition)

}

/**
  * Consumer Detail JSON Companion Object
  * @author lawrence.daniels@gmail.com
  */
object ConsumerDetailJs {

  implicit val ConsumerDetailReads = Json.reads[ConsumerDetailJs]

  implicit val ConsumerDetailWrites = Json.writes[ConsumerDetailJs]

  case class ConsumerDeltaKey(consumerId: String, topic: String, partition: Int)

}
