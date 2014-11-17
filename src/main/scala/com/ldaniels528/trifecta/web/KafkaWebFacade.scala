package com.ldaniels528.trifecta.web

import com.ldaniels528.trifecta.io.kafka.{Broker, KafkaMicroConsumer}
import com.ldaniels528.trifecta.io.zookeeper.ZKProxy
import net.liftweb.json.{Extraction, JValue}

/**
 * Kafka Web Facade
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class KafkaWebFacade(zk: ZKProxy, correlationId: Int = 0) {
  implicit val formats = net.liftweb.json.DefaultFormats
  implicit val zkProxy: ZKProxy = zk

  /**
   * Returns a collection of brokers
   * @return a collection of brokers
   */
  def brokers: Seq[Broker] = {
    KafkaMicroConsumer.getBrokerList(zk) map (b => Broker(b.host, b.port))
  }

  def getBrokers: JValue = Extraction.decompose(brokers)

  def getTopics: JValue = {
    val topicData = KafkaMicroConsumer.getTopicList(brokers, correlationId)
    Extraction.decompose(topicData)
  }

  def getTopicByName(topic: String): Option[JValue] = {
    KafkaMicroConsumer
      .getTopicList(brokers, correlationId)
      .find(_.topic == topic)
      .map(Extraction.decompose(_))
  }

  case class TopicJson()

}
