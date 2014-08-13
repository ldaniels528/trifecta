package com.ldaniels528.verify.subsystems.kafka

import java.util.Properties

import com.ldaniels528.verify.util.VerifyUtils._
import kafka.javaapi.producer.Producer
import kafka.producer.{KeyedMessage, ProducerConfig}

/**
 * Verify Kafka Publisher
 * @author lawrence.daniels@gmail.com
 */
class KafkaPublisher(config: ProducerConfig) {
  private var producer: Option[Producer[Array[Byte], Array[Byte]]] = None

  /**
   * Opens the connection to the message publisher
   */
  def open() {
    producer = Some(new Producer(config))
  }

  /**
   * Shuts down the connection to the message publisher
   */
  def close() {
    producer.foreach(_.close)
  }

  /**
   * Transports a message to the messaging server
   * @param topic the given topic name (e.g. "flights")
   * @param key the given message key
   * @param value the given message value
   */
  def publish(topic: String, key: Array[Byte], value: Array[Byte]) {
    producer.foreach(_.send(new KeyedMessage(topic, key, value)))
  }

}

/**
 * Verify Kafka Publisher
 * @author lawrence.daniels@gmail.com
 */
object KafkaPublisher {

  def apply(brokers: Seq[Broker]): KafkaPublisher = {
    val m = Map("metadata.broker.list" -> mkBrokerList(brokers),
      "key.serializer.class" -> "kafka.serializer.DefaultEncoder",
      "serializer.class" -> "kafka.serializer.DefaultEncoder",
      "partitioner.class" -> "kafka.producer.DefaultPartitioner",
      "request.required.acks" -> "1",
      "compression.codec" -> "none")
    new KafkaPublisher(new ProducerConfig(m.toProps))
  }

  def apply(brokers: Seq[Broker], p: Properties): KafkaPublisher = {
    new KafkaPublisher(new ProducerConfig(p))
  }

  private def mkBrokerList(brokers: Seq[Broker]): String = {
    brokers map (b => s"${b.host}:${b.port}") mkString ","
  }

}