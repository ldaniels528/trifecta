package com.ldaniels528.verify.modules.kafka

import akka.actor.ActorRef
import com.ldaniels528.verify.io.{Compression, EndPoint}
import com.ldaniels528.verify.modules.kafka.KafkaStreamingConsumer.{StreamedMessage, StreamingMessageConsumer}
import com.ldaniels528.verify.util.VxUtils._
import kafka.consumer.{Consumer, ConsumerConfig}

import scala.concurrent.{ExecutionContext, Future}

/**
 * Kafka Streaming Consumer
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class KafkaStreamingConsumer(consumerConfig: ConsumerConfig) extends Compression {
  private val consumer = Consumer.create(consumerConfig)

  /**
   * Streams data from a Kafka source
   * @param topic the given topic name
   * @param parallelism the given number of processing threads
   * @param listener the observer to call back upon receipt of a new message
   */
  def observe(topic: String, parallelism: Int, listener: StreamingMessageConsumer)(implicit ec: ExecutionContext) {
    val streamMap = consumer.createMessageStreams(Map(topic -> parallelism))

    // now create an object to consume the messages
    streamMap.get(topic) foreach { streams =>
      streams foreach { stream =>
        Future {
          val it = stream.iterator()
          while (it.hasNext()) {
            val mam = it.next()
            listener.consume(new StreamedMessage(mam.topic, mam.partition, mam.offset, mam.key(), mam.message()))
          }
        }
      }
    }
  }

  /**
   * Streams data from a Kafka topic to an Akka actor
   * @param topic the given topic name
   * @param parallelism the given number of processing threads
   * @param actor the given actor reference
   */
  def stream(topic: String, parallelism: Int, actor: ActorRef)(implicit ec: ExecutionContext) {
    val streamMap = consumer.createMessageStreams(Map(topic -> parallelism))

    // now create an object to consume the messages
    streamMap.get(topic) foreach { streams =>
      streams foreach { stream =>
        Future {
          val it = stream.iterator()
          while (it.hasNext()) {
            val mam = it.next()
            actor ! StreamedMessage(mam.topic, mam.partition, mam.offset, mam.key(), mam.message())
          }
        }
      }
    }
  }

}

/**
 * Kafka Streaming Consumer Companion Object
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
object KafkaStreamingConsumer {

  def apply(zkEndPoint: EndPoint, groupId: String): KafkaStreamingConsumer = {
    val consumerConfig = new ConsumerConfig(
      Map("zookeeper.connect" -> zkEndPoint.host,
        "group.id" -> groupId,
        "zookeeper.session.timeout.ms" -> "400",
        "zookeeper.sync.time.ms" -> "200",
        "auto.commit.interval.ms" -> "1000").toProps)
    new KafkaStreamingConsumer(consumerConfig)
  }

  case class StreamedMessage(topic: String, partition: Int, offset: Long, key: Array[Byte], message: Array[Byte])

  /**
   * This trait is implemented by classes that are interested in
   * consuming Kafka messages.
   * @author Lawrence Daniels <lawrence.daniels@gmail.com>
   */
  trait StreamingMessageConsumer {

    /**
     * Called when data is ready to be consumed
     * @param message the message as a binary string
     */
    def consume(message: StreamedMessage)

  }

}