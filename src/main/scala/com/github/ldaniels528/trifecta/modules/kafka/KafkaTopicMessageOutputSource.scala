package com.github.ldaniels528.trifecta.modules.kafka

import com.github.ldaniels528.commons.helpers.OptionHelper.Risky._
import com.github.ldaniels528.trifecta.io.{KeyAndMessage, MessageOutputSource}
import com.github.ldaniels528.trifecta.messages.MessageDecoder

import scala.concurrent.ExecutionContext

/**
 * Kafka Topic Output Source
 * @author lawrence.daniels@gmail.com
 */
class KafkaTopicMessageOutputSource(brokers: Seq[Broker], outputTopic: String) extends MessageOutputSource {
  private var publisher_? : Option[KafkaPublisher] = None

  override def open() = {
    publisher_? = KafkaPublisher(brokers)
    publisher_?.foreach(_.open())
  }

  override def write(data: KeyAndMessage, decoder: Option[MessageDecoder[_]])(implicit ec: ExecutionContext) {
    publisher_?.foreach(_.publish(outputTopic, data.key, data.message))
  }

  override def close() = publisher_?.foreach(_.close())

}
