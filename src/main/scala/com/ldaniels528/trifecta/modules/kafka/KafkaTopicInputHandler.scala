package com.ldaniels528.trifecta.modules.kafka

import com.ldaniels528.trifecta.support.io.{InputHandler, KeyAndMessage}
import com.ldaniels528.trifecta.support.kafka.{Broker, KafkaMicroConsumer}
import kafka.common.TopicAndPartition

/**
 * Kafka Topic Input Source
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class KafkaTopicInputHandler(brokers: Seq[Broker], topic: String, partition: Int = 0, fetchSize: Int = 65536)
  extends InputHandler {
  private val consumer = new KafkaMicroConsumer(TopicAndPartition(topic, partition), brokers, correlationId = 0)
  private var offset_? : Option[Long] = consumer.getFirstOffset

  /**
   * Reads the given keyed-message from the underlying stream
   * @return a [[KeyAndMessage]]
   */
  override def read: Option[KeyAndMessage] = {
    for {
      offset <- offset_?
      md <- consumer.fetch(offset, 65536).headOption
    } yield {
      offset_? = Option(md.nextOffset)
      KeyAndMessage(md.key, md.message)
    }
  }

  /**
   * Closes the underlying stream
   */
  override def close() = consumer.close()

}