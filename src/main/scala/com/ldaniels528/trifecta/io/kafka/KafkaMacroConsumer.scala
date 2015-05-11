package com.ldaniels528.trifecta.io.kafka

import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}

import akka.actor.ActorRef
import com.ldaniels528.trifecta.messages.logic.Condition
import com.ldaniels528.commons.helpers.PropertiesHelper._
import kafka.consumer.{Consumer, ConsumerConfig}

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success, Try}

/**
 * Kafka High-Level Message Consumer
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class KafkaMacroConsumer(consumerConfig: ConsumerConfig) {
  private val consumer = Consumer.create(consumerConfig)

  /**
   * Closes the consumer's connection
   */
  def close(): Unit = consumer.shutdown()

  /**
   * Counts the total number of occurrences of messages matching the given criteria for a Kafka topic
   * @param topic the given topic name
   * @param parallelism the given number of processing threads
   * @param conditions the given collection of acceptance criteria
   * @return a promise of the total message count
   */
  def count(topic: String, parallelism: Int, conditions: Condition*)(implicit ec: ExecutionContext): Future[Long] = {
    val streamMap = consumer.createMessageStreams(Map(topic -> parallelism))
    val promise = Promise[Long]()
    val total = new AtomicLong(0L)

    // now create an object to consume the messages
    val tasks = (streamMap.get(topic) map { streams =>
      streams map { stream =>
        Future {
          Try {
            val it = stream.iterator()
            while (it.hasNext()) {
              val mam = it.next()
              if (conditions.forall(_.satisfies(mam.message(), mam.key()))) total.incrementAndGet()
            }
          }
        }
      }
    }).toList.flatten

    // check for the failure to find the message by key
    Future.sequence(tasks).onComplete {
      case Success(v) => promise.success(total.get)
      case Failure(e) => promise.failure(e)
    }

    promise.future
  }

  /**
   * Iterates over messages from a Kafka topic
   * @param topic the given topic name
   * @param parallelism the given number of processing threads
   */
  def iterate(topic: String, parallelism: Int): Iterator[StreamedMessage] = {
    val streamMap = consumer.createMessageStreams(Map(topic -> parallelism))
    val streams = streamMap.getOrElse(topic, Nil) map (_.iterator())
    new Iterator[StreamedMessage] {
      override def hasNext: Boolean = streams.exists(_.hasNext())

      override def next(): StreamedMessage = {
        (streams.find(_.hasNext()) map { consumerIterator =>
          val mam = consumerIterator.next()
          StreamedMessage(mam.topic, mam.partition, mam.offset, mam.key(), mam.message())
        }).getOrElse(throw new IllegalStateException("Unexpected end of stream"))
      }
    }
  }

  /**
   * Streams messages from a Kafka topic to an observer
   * @param topic the given topic name
   * @param parallelism the given number of processing threads
   * @param observer the observer to callback upon receipt of a new message
   */
  def observe(topic: String, parallelism: Int)(observer: StreamedMessage => Unit)(implicit ec: ExecutionContext) {
    val streamMap = consumer.createMessageStreams(Map(topic -> parallelism))

    // now create an object to consume the messages
    streamMap.get(topic) foreach { streams =>
      streams foreach { stream =>
        Future {
          Try {
            val it = stream.iterator()
            while (it.hasNext()) {
              val mam = it.next()
              observer(StreamedMessage(mam.topic, mam.partition, mam.offset, mam.key(), mam.message()))
            }
          }
        }
      }
    }
  }

  /**
   * Scans a Kafka topic for the first occurrence of a message matching the given criteria
   * @param topic the given topic name
   * @param parallelism the given number of processing threads
   * @param conditions the given collection of acceptance criteria
   * @return a promise of an option of a streamed message
   */
  def scan(topic: String, parallelism: Int, conditions: Condition*)(implicit ec: ExecutionContext): Future[Option[StreamedMessage]] = {
    val streamMap = consumer.createMessageStreams(Map(topic -> parallelism))
    val promise = Promise[Option[StreamedMessage]]()
    val found = new AtomicBoolean(false)
    var message: Option[StreamedMessage] = None

    // now create an object to consume the messages
    val tasks = (streamMap.get(topic) map { streams =>
      streams map { stream =>
        Future {
          Try {
            val it = stream.iterator()
            while (!found.get && it.hasNext()) {
              val mam = it.next()
              if (!found.get && conditions.forall(_.satisfies(mam.message(), mam.key()))) {
                if (found.compareAndSet(false, true)) {
                  message = Option(StreamedMessage(mam.topic, mam.partition, mam.offset, mam.key(), mam.message()))
                }
              }
            }
          }
        }
      }
    }).toList.flatten

    // check for the failure to find a message
    Future.sequence(tasks).onComplete {
      case Success(v) => promise.success(message)
      case Failure(e) => promise.failure(e)
    }

    promise.future
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
          Try {
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

}

/**
 * Kafka Streaming Consumer Companion Object
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
object KafkaMacroConsumer {

  /**
   * Convenience method   for creating Streaming Consumer instances
   * @param connectionString the given Zookeeper connection string (e.g. "localhost:2181")
   * @param groupId the given consumer group ID
   * @return a new Streaming Consumer instance
   * @see http://kafka.apache.org/07/configuration.html
   */
  def apply(connectionString: String, groupId: String, params: (String, Any)*): KafkaMacroConsumer = {
    val props = Map(
      "zookeeper.connect" -> connectionString,
      "group.id" -> groupId,
      "zookeeper.session.timeout.ms" -> "400",
      "zookeeper.sync.time.ms" -> "200",
      "auto.commit.interval.ms" -> "1000") ++ Map(params.map { case (k, v) => (k, String.valueOf(v))}: _*)
    new KafkaMacroConsumer(new ConsumerConfig(props.toProps))
  }

}