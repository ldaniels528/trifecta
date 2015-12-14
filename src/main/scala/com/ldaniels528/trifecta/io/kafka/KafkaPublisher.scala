package com.ldaniels528.trifecta.io.kafka

import java.util.Properties

import com.ldaniels528.commons.helpers.PropertiesHelper._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

/**
 * Kafka Publisher
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class KafkaPublisher(config: Properties) {
  private var producer: Option[KafkaProducer[Array[Byte], Array[Byte]]] = None

  /**
   * Opens the connection to the message publisher
   */
  def open() {
    producer = Option(new KafkaProducer(config))
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
  def publish(topic: String, key: Array[Byte], message: Array[Byte]) = {
    producer match {
      case Some(kp) => kp.send(new ProducerRecord(topic, key, message))
      case None =>
        throw new IllegalStateException("No connection established. Use open() to connect.")
    }
  }

  /**
   * Transports a message to the messaging server
   * @param rec the given [[ProducerRecord producer record]]
   */
  def publish(rec: ProducerRecord[Array[Byte], Array[Byte]]) = {
    producer match {
      case Some(kp) => kp.send(rec)
      case None =>
        throw new IllegalStateException("No connection established. Use open() to connect.")
    }
  }

}

/**
 * Verify Kafka Publisher
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
object KafkaPublisher {

  def apply(brokers: Seq[Broker]): KafkaPublisher = {
    val m = Map[String, Object](
      ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> mkBrokerList(brokers),
      ProducerConfig.RETRIES_CONFIG -> "3",
      ProducerConfig.ACKS_CONFIG -> "all",
      ProducerConfig.COMPRESSION_TYPE_CONFIG -> "none",
      ProducerConfig.BATCH_SIZE_CONFIG -> (200: Integer),
      ProducerConfig.BLOCK_ON_BUFFER_FULL_CONFIG -> (true: java.lang.Boolean),
      ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.ByteArraySerializer",
      ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.ByteArraySerializer")
    new KafkaPublisher(m.toProps)
  }

  def apply(brokers: Seq[Broker], p: Properties): KafkaPublisher = new KafkaPublisher(p)

  private def mkBrokerList(brokers: Seq[Broker]): String = {
    brokers map (b => s"${b.host}:${b.port}") mkString ","
  }

}