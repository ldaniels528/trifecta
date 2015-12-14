package com.github.ldaniels528.trifecta.models

import play.api.libs.json.Json

/**
  * Topic Partition JSON model
  * @author lawrence.daniels@gmail.com
  */
case class TopicPartitionJs(partition: Int,
                            startOffset: Option[Long],
                            endOffset: Option[Long],
                            messages: Option[Long],
                            leader: Option[BrokerJs] = None,
                            replicas: Seq[BrokerJs] = Nil)

/**
  * Topic Partition JSON Companion Object
  * @author lawrence.daniels@gmail.com
  */
object TopicPartitionJs {

  implicit val TopicPartitionReads = Json.reads[TopicPartitionJs]

  implicit val TopicPartitionWrites = Json.writes[TopicPartitionJs]

}
