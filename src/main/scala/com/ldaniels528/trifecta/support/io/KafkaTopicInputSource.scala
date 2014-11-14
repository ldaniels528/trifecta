package com.ldaniels528.trifecta.support.io

import com.ldaniels528.trifecta.support.io.query.QuerySource
import com.ldaniels528.trifecta.support.kafka.{Broker, KafkaMicroConsumer, KafkaQuerySource}
import com.ldaniels528.trifecta.support.zookeeper.ZKProxy
import kafka.common.TopicAndPartition

/**
 * Kafka Topic Input Source
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class KafkaTopicInputSource(brokers: Seq[Broker], topic: String, partition: Int = 0, fetchSize: Int = 2048, correlationId: Int = 0)(implicit zk: ZKProxy)
  extends InputSource {
  private val consumer = new KafkaMicroConsumer(TopicAndPartition(topic, partition), brokers, correlationId)
  private var offset_? : Option[Long] = consumer.getFirstOffset

  /**
   * Reads the given keyed-message from the underlying stream
   * @return a [[KeyAndMessage]]
   */
  override def read: Option[KeyAndMessage] = {
    for {
      offset <- offset_?
      md <- consumer.fetch(offset, fetchSize).headOption
    } yield {
      offset_? = offset_? map (_ + 1)
      KeyAndMessage(md.key, md.message)
    }
  }

  /**
   * Returns a source for querying via Big Data Query Language (BDQL)
   * @return the option of a query source
   */
  override def getQuerySource: Option[QuerySource] = Option(KafkaQuerySource(topic, brokers, correlationId))

  /**
   * Closes the underlying stream
   */
  override def close() = consumer.close()

}