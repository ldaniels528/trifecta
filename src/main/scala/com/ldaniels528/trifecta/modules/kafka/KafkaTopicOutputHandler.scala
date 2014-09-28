package com.ldaniels528.trifecta.modules.kafka

import com.ldaniels528.trifecta.support.io.BinaryOutputHandler
import com.ldaniels528.trifecta.support.kafka.{Broker, KafkaPublisher}

import scala.concurrent.ExecutionContext

/**
 * Kafka Topic Output Writer
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class KafkaTopicOutputHandler(brokers: Seq[Broker], outputTopic: String) extends BinaryOutputHandler {
  private val publisher = KafkaPublisher(brokers)
  publisher.open()

  /**
   * Writes the given key-message pair to the underlying stream
   * @param key the given key
   * @param message the given message
   * @return the response value
   */
  override def write(key: Array[Byte], message: Array[Byte])(implicit ec: ExecutionContext) {
    publisher.publish(outputTopic, key, message)
  }

  override def close() = publisher.close()


}
