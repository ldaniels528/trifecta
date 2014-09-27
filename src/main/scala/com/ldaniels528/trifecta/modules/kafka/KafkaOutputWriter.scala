package com.ldaniels528.trifecta.modules.kafka

import com.ldaniels528.trifecta.modules.io.OutputWriter
import com.ldaniels528.trifecta.support.kafka.KafkaMicroConsumer.MessageData
import com.ldaniels528.trifecta.support.kafka.{Broker, KafkaPublisher}

import scala.concurrent.ExecutionContext

/**
 * Kafka Output Writer
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class KafkaOutputWriter(brokers: Seq[Broker], outputTopic: String) extends OutputWriter {
  private val publisher = KafkaPublisher(brokers)
  publisher.open()

  override def write(md: MessageData)(implicit ec: ExecutionContext) {
    publisher.publish(outputTopic, md.key, md.message)
  }

  override def close() = publisher.close()

}
