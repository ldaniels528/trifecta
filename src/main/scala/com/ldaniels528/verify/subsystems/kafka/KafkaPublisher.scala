package com.ldaniels528.verify.subsystems.kafka

import kafka.javaapi.producer.Producer
import kafka.producer.KeyedMessage
import kafka.producer.ProducerConfig

/**
 * Verify Kafka Message Publisher
 * @author lawrence.daniels@gmail.com
 */
class KafkaPublisher() {
  private var producer: Producer[String, Array[Byte]] = _

  /**
   * Opens the connection to the message publisher
   */
  def open(brokers: Seq[Broker], partitionerClass: Option[String] = None) {
    producer = new Producer(mkConfig(brokers, partitionerClass));
  }

  /**
   * Shuts down the connection to the message publisher
   */
  def close() {
    Option(producer) foreach (_.close)
    producer = null
  }

  /**
   * Transports a message to the messaging server
   * @param topic the given topic name (e.g. "flights")
   * @param key the given message key
   * @param value the given message value
   */
  def publish(topic: String, key: String, value: Array[Byte]) {
    Option(producer) foreach (_.send(new KeyedMessage(topic, key, value)))
  }

  /**
   * Creates a new producer configuration instance
   * @param partitionerClass
   * @param brokers
   * @return
   */
  def mkConfig(brokers: Seq[Broker], partitionerClass: Option[String] = None): ProducerConfig = {
    val props = new java.util.Properties()
    props.put("metadata.broker.list", mkBrokerList(brokers))
    //props.put("serializer.class", "kafka.serializer.StringEncoder")
    props.put("serializer.class", "kafka.serializer.DefaultEncoder");
    partitionerClass foreach (props.put("partitioner.class", _))
    props.put("key.serializer.class", "kafka.serializer.StringEncoder")
    props.put("request.required.acks", "1")
    new ProducerConfig(props)
  }

  private def mkBrokerList(brokers: Seq[Broker]): String = {
    brokers map (b => s"${b.host}:${b.port}") mkString (",")
  }

}