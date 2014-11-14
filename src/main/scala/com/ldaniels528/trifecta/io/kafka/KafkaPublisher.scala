package com.ldaniels528.trifecta.io.kafka

import java.util.Properties

import com.ldaniels528.trifecta.util.TxUtils._
import kafka.javaapi.producer.Producer
import kafka.producer.{KeyedMessage, ProducerConfig}

/**
 * Kafka Publisher
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class KafkaPublisher(config: ProducerConfig) {
  private var producer: Option[Producer[Array[Byte], Array[Byte]]] = None

  /**
   * Opens the connection to the message publisher
   */
  def open() {
    producer = Option(new Producer(config))
  }

  /**
   * Shuts down the connection to the message publisher
   */
  def close() {
    producer.foreach(_.close)
    producer = None
  }

  /**
   * Transports a message to the messaging server
   * @param topic the given topic name (e.g. "greetings")
   * @param key the given message key
   * @param message the given message payload
   */
  def publish(topic: String, key: Array[Byte], message: Array[Byte]) {
    producer.foreach(_.send(new KeyedMessage(topic = topic, key = key, message = message)))
  }

  /**
   * Transports a message to the messaging server
   * @param km the given keyed message value
   */
  def publish(km: KeyedMessage[Array[Byte], Array[Byte]]): Unit = producer.foreach(_.send(km))

}

/**
 * Verify Kafka Publisher
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
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