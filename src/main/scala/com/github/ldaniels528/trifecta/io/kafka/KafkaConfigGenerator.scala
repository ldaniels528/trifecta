package com.github.ldaniels528.trifecta.io.kafka

import java.util.Properties

import com.github.ldaniels528.trifecta.io.kafka.KafkaZkUtils.BrokerDetails
import org.apache.kafka.common.serialization.StringDeserializer

/**
  * Kafka Configuration Generator
  * @author lawrence.daniels@gmail.com
  */
object KafkaConfigGenerator {

  def getConsumerProperties(brokers: Seq[BrokerDetails], groupId: String, autoOffsetReset: Option[String] = None): Properties = {
    val bootstrapServers = brokers map (b => s"${b.host}:${b.port}") mkString ","
    val props = new Properties()
    brokers.map(_.version).max match {
      case V0_8_x =>
      case V0_9_x =>
      case V0_10_x =>
      case _ =>
    }

    props.put("bootstrap.servers", bootstrapServers)
    props.put("group.id", groupId)
    autoOffsetReset.foreach(props.put("auto.offset.reset", _))
    props.put("key.deserializer", classOf[StringDeserializer].getName)
    props.put("value.deserializer", classOf[StringDeserializer].getName)
    props
  }

  val V0_8_x = 0
  val V0_9_x = 1
  val V0_10_x = 2

}
