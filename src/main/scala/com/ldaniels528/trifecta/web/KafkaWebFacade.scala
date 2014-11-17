package com.ldaniels528.trifecta.web

import com.ldaniels528.trifecta.io.kafka.{Broker, KafkaMicroConsumer}
import com.ldaniels528.trifecta.io.zookeeper.ZKProxy
import com.ldaniels528.trifecta.util.ResourceHelper._
import com.ldaniels528.trifecta.web.KafkaWebFacade._
import kafka.common.TopicAndPartition
import net.liftweb.json.{Extraction, JValue}

/**
 * Kafka Web Facade
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class KafkaWebFacade(zk: ZKProxy, correlationId: Int = 0) {
  implicit val formats = net.liftweb.json.DefaultFormats
  implicit val zkProxy: ZKProxy = zk

  val brokers: Seq[Broker] = KafkaMicroConsumer.getBrokerList(zk) map (b => Broker(b.host, b.port))

  def getBrokers: JValue = Extraction.decompose(brokers)

  def getTopics: JValue = Extraction.decompose(KafkaMicroConsumer.getTopicList(brokers, correlationId))

  def getTopicSummaries: JValue = {
    Extraction.decompose(KafkaMicroConsumer.getTopicList(brokers, correlationId).groupBy(_.topic) map { case (topic, details) =>
      // produce the partitions
      val partitions = details map { detail =>
        new KafkaMicroConsumer(TopicAndPartition(topic, detail.partitionId), brokers) use { consumer =>
          // get the start and end offsets and message count
          val startOffset = consumer.getFirstOffset
          val endOffset = consumer.getLastOffset
          val messages = for {start <- startOffset; end <- endOffset} yield Math.max(0L, end - start)

          // create the topic partition
          TopicPartition(detail.partitionId, startOffset, endOffset, messages, detail.leader, detail.replicas)
        }
      }

      // get the total message count
      TopicSummary(topic, partitions, totalMessages = partitions.flatMap(_.messages).sum)
    })
  }

  def getTopicByName(topic: String): Option[JValue] = {
    KafkaMicroConsumer.getTopicList(brokers, correlationId).find(_.topic == topic).map(Extraction.decompose)
  }

  def getTopicDetailsByName(topic: String): JValue = {
    Extraction.decompose(KafkaMicroConsumer.getTopicPartitions(topic) map { partition =>
      new KafkaMicroConsumer(TopicAndPartition(topic, partition), brokers) use { consumer =>
        val startOffset = consumer.getFirstOffset
        val endOffset = consumer.getLastOffset
        val messages = for {start <- startOffset; end <- endOffset} yield Math.max(0L, end - start)
        TopicDetails(topic, partition, startOffset, endOffset, messages)
      }
    })
  }

}

/**
 * Kafka Web Facade Singleton
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
object KafkaWebFacade {

  case class TopicDetails(topic: String, partition: Int, startOffset: Option[Long], endOffset: Option[Long], messages: Option[Long])

  case class TopicSummary(topic: String, partitions: Seq[TopicPartition], totalMessages: Long)

  case class TopicPartition(partition: Int, startOffset: Option[Long], endOffset: Option[Long], messages: Option[Long], leader: Option[Broker], replicas: Seq[Broker])

}
