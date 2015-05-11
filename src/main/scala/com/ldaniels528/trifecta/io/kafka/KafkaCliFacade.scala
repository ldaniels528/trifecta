package com.ldaniels528.trifecta.io.kafka

import java.util.Date

import com.ldaniels528.trifecta.TxConfig
import com.ldaniels528.trifecta.io.AsyncIO.IOCounter
import com.ldaniels528.trifecta.io.avro.AvroDecoder
import com.ldaniels528.trifecta.io.kafka.KafkaCliFacade._
import com.ldaniels528.trifecta.io.kafka.KafkaMicroConsumer._
import com.ldaniels528.trifecta.io.zookeeper.ZKProxy
import com.ldaniels528.trifecta.io.{AsyncIO, KeyAndMessage, OutputSource}
import com.ldaniels528.trifecta.messages.logic.Condition
import com.ldaniels528.trifecta.messages.{BinaryMessage, MessageCursor, MessageDecoder}
import com.ldaniels528.commons.helpers.ResourceHelper._
import kafka.common.TopicAndPartition

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

/**
 * Kafka CLI Facade
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class KafkaCliFacade(config: TxConfig) {
  private var publisher_? : Option[KafkaPublisher] = None

  // set user defined Kafka root directory
  KafkaMicroConsumer.rootKafkaPath = config.kafkaRootPath

  /**
   * Returns a collection of brokers
   * @param zk the given [[ZKProxy]] instance
   * @return a collection of brokers
   */
  def brokers(implicit zk: ZKProxy): Seq[Broker] = {
    KafkaMicroConsumer.getBrokerList map (b => Broker(b.host, b.port))
  }

  /**
   * Commits the offset for a given topic and group ID
   */
  def commitOffset(topic: String, partition: Int, groupId: String, offset: Long, metadata: Option[String] = None)(implicit zk: ZKProxy) {
    new KafkaMicroConsumer(TopicAndPartition(topic, partition), brokers) use (
      _.commitOffsets(groupId, offset, metadata getOrElse "N/A"))
  }

  /**
   * Counts the messages matching a given condition [references cursor]
   */
  def countMessages(topic: String, conditions: Seq[Condition], decoder: Option[MessageDecoder[_]])(implicit zk: ZKProxy, ec: ExecutionContext): Future[Long] = {
    KafkaMicroConsumer.count(topic, brokers, conditions: _*)
  }

  /**
   * Returns the offsets for a given topic and group ID
   */
  def fetchOffsets(topic: String, partition: Int, groupId: String)(implicit zk: ZKProxy): Option[Long] = {
    new KafkaMicroConsumer(TopicAndPartition(topic, partition), brokers) use (_.fetchOffsets(groupId))
  }

  /**
   * Finds messages that corresponds to the given criteria and exports them to a topic
   * @example kfind frequency > 5000 -o topic:highFrequency.quotes
   */
  def findMessages(topic: String, decoder: Option[MessageDecoder[_]], conditions: Seq[Condition], outputHandler: OutputSource)(implicit zk: ZKProxy, ec: ExecutionContext): AsyncIO = {
    // define the counter
    val counter = IOCounter(System.currentTimeMillis())

    // find and export the messages matching our criteria
    val task = KafkaMicroConsumer.observe(topic, brokers) { md =>
      counter.updateReadCount(1)
      if (conditions.forall(_.satisfies(md.message, md.key))) {
        outputHandler.write(KeyAndMessage(md.key, md.message), decoder)
        counter.updateWriteCount(1)
        ()
      }
    }

    // upon completion, close the output device and return the count
    task.foreach { u =>
      outputHandler.close()
    }

    AsyncIO(task, counter)
  }

  /**
   * Retrieves the list of Kafka consumers
   */
  def getConsumers(consumerPrefix: Option[String], topicPrefix: Option[String], includePartitionManager: Boolean)(implicit zk: ZKProxy, ec: ExecutionContext): Future[List[ConsumerDelta]] = {
    // get the Kafka consumer groups
    val consumersCG = Future {
      KafkaMicroConsumer.getConsumerDetails(topicPrefix) map { c =>
        val topicOffset = getLastOffset(c.topic, c.partition)
        val delta = topicOffset map (offset => Math.max(0L, offset - c.offset))
        ConsumerDelta(c.consumerId, c.topic, c.partition, c.offset, topicOffset, delta, c.lastModified.map(new Date(_)))
      }
    }

    // get the Kafka Spout consumers (Partition Manager)
    val consumersPM = if (!includePartitionManager) Future.successful(Nil)
    else Future {
      KafkaMicroConsumer.getStormConsumerList() map { c =>
        val topicOffset = getLastOffset(c.topic, c.partition)
        val delta = topicOffset map (offset => Math.max(0L, offset - c.offset))
        ConsumerDelta(c.topologyName, c.topic, c.partition, c.offset, topicOffset, delta, c.lastModified.map(new Date(_)))
      }
    }

    // combine the futures for the two lists
    (for {
      consumersA <- consumersCG
      consumersB <- consumersPM
    } yield consumersA.toList ::: consumersB.toList)
      .map {
      _.filter(c => contentFilter(consumerPrefix, c.consumerId))
        .filter(c => contentFilter(topicPrefix, c.topic))
        .sortBy(c => (c.consumerId, c.topic, c.partition))
    }
  }

  /**
   * Returns the first offset for a given topic
   */
  def getFirstOffset(topic: String, partition: Int)(implicit zk: ZKProxy): Option[Long] = {
    new KafkaMicroConsumer(TopicAndPartition(topic, partition), brokers) use (_.getFirstOffset)
  }

  /**
   * Returns the last offset for a given topic
   */
  def getLastOffset(topic: String, partition: Int)(implicit zk: ZKProxy): Option[Long] = {
    new KafkaMicroConsumer(TopicAndPartition(topic, partition), brokers) use (_.getLastOffset)
  }

  /**
   * Decodes the given message
   * @param messageData the given option of a message
   * @param aDecoder the given message decoder
   * @return the decoded message
   */
  private def decodeMessage(messageData: Option[BinaryMessage], aDecoder: MessageDecoder[_]): Option[Seq[AvroRecord]] = {
    // only Avro decoders are supported
    val decoder: AvroDecoder = aDecoder match {
      case avDecoder: AvroDecoder => avDecoder
      case _ => throw new IllegalStateException("Only Avro decoding is supported")
    }

    // decode the message
    for {
      md <- messageData
      rec = decoder.decode(md.message) match {
        case Success(record) =>
          val fields = record.getSchema.getFields.asScala.map(_.name.trim).toSeq
          fields map { f =>
            val v = record.get(f)
            AvroRecord(f, v, Option(v) map (_.getClass.getSimpleName) getOrElse "")
          }
        case Failure(e) =>
          throw new IllegalStateException(e.getMessage, e)
      }
    } yield rec
  }

  /**
   * Returns the message key for a given topic partition and offset
   */
  def getMessageKey(topic: String, partition: Int, offset: Long, fetchSize: Int)(implicit zk: ZKProxy): Option[Array[Byte]] = {
    new KafkaMicroConsumer(TopicAndPartition(topic, partition), brokers) use { consumer =>
      consumer.fetch(offset)(fetchSize).headOption map (_.key)
    }
  }

  /**
   * Returns the size of the message for a given topic partition and offset
   */
  def getMessageSize(topic: String, partition: Int, offset: Long, fetchSize: Int)(implicit zk: ZKProxy): Option[Int] = {
    new KafkaMicroConsumer(TopicAndPartition(topic, partition), brokers) use {
      _.fetch(offset.toLong)(fetchSize).headOption map (_.message.length)
    }
  }

  /**
   * Returns the minimum and maximum message size for a given topic partition and offset range
   */
  def getMessageMinMaxSize(topic: String, partition: Int, startOffset: Long, endOffset: Long, fetchSize: Int)(implicit zk: ZKProxy): Seq[MessageMaxMin] = {
    new KafkaMicroConsumer(TopicAndPartition(topic, partition), brokers) use { consumer =>
      val offsets = startOffset.toLong to endOffset.toLong
      val messages = consumer.fetch(offsets: _*)(fetchSize).map(_.message.length)
      if (messages.nonEmpty) Seq(MessageMaxMin(messages.min, messages.max)) else Nil
    }
  }

  /**
   * Generates statistics for the partition range of a given topic
   * @param topic the given topic (e.g. com.shocktrade.quotes.realtime)
   * @param partition0 the starting partition
   * @param partition1 the ending partition
   * @return an iteration of statistics
   */
  def getStatisticsData(topic: String, partition0: Int, partition1: Int)(implicit zk: ZKProxy): Iterable[TopicOffsets] = {
    for {
      partition <- partition0 to partition1
      first <- getFirstOffset(topic, partition)
      last <- getLastOffset(topic, partition)
    } yield TopicOffsets(topic, partition, first, last, Math.max(0, last - first))
  }

  /**
   * Returns a list of topics
   */
  def getTopics(prefix: Option[String], detailed: Boolean)(implicit zk: ZKProxy): Either[Seq[TopicItem], Seq[TopicItemCompact]] = {
    // get the raw topic data
    val topicData = KafkaMicroConsumer.getTopicList(brokers)

    // is the detailed list flag set?
    if (detailed) {
      Left {
        topicData flatMap { t =>
          val item = TopicItem(t.topic, t.partitionId, t.leader map (_.toString) getOrElse "N/A", t.replicas.size, t.isr.size)
          if (prefix.isEmpty || prefix.exists(t.topic.startsWith)) Some(item) else None
        }
      }
    }

    // otherwise, create a detailed output
    else {
      Right {
        topicData.groupBy(_.topic).toSeq flatMap { case (name, details) =>
          val partitions = details.map(_.partitionId)
          val inSync = {
            val replicas = details.flatMap(_.replicas).length
            val isr = details.flatMap(_.isr).length
            if (replicas != 0) 100 * (isr.toDouble / replicas.toDouble) else 0
          }
          val item = TopicItemCompact(name, partitions.max + 1, f"$inSync%.0f%%")
          if (prefix.isEmpty || prefix.exists(name.startsWith)) Some(item) else None
        }
      }
    }
  }

  /**
   * Returns a tuple containing the minimum and maximum partition indices respectively for the given topic
   * @param topic the given topic name
   * @return a tuple containing the minimum and maximum partition indices
   */
  def getTopicPartitionRange(topic: String)(implicit zk: ZKProxy): Option[(Int, Int)] = {
    val partitions = KafkaMicroConsumer.getTopicPartitions(topic)
    if (partitions.isEmpty) None else Option((partitions.min, partitions.max))
  }

  /**
   * Publishes the given message to the given topic
   */
  def publishMessage(topic: String, key: Array[Byte], message: Array[Byte])(implicit zk: ZKProxy): Unit = {
    // if the publisher has not been created ...
    if (publisher_?.isEmpty) publisher_? = Option {
      val publisher = KafkaPublisher(brokers)
      publisher.open()
      publisher
    }

    // publish the message
    publisher_? foreach (_.publish(topic, key, message))
  }

  /**
   * Sets the offset of a consumer group ID to zero for all partitions
   */
  def resetConsumerGroup(topic: String, groupId: String)(implicit zk: ZKProxy): Unit = {
    // get the partition range
    val partitions = KafkaMicroConsumer.getTopicPartitions(topic)
    if (partitions.isEmpty)
      throw new IllegalStateException(s"No partitions found for topic $topic")
    val (start, end) = (partitions.min, partitions.max)

    // reset the consumer group ID for each partition
    (start to end) foreach { partition =>
      new KafkaMicroConsumer(TopicAndPartition(topic, partition), brokers) use { consumer =>
        consumer.getFirstOffset foreach { offset =>
          consumer.commitOffsets(groupId, offset, "resetting consumer ID")
        }
      }
    }
  }

}

/**
 * Kafka Facade Singleton
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
object KafkaCliFacade {

  case class AvroRecord(field: String, value: Any, `type`: String)

  case class AvroVerification(verified: Int, failed: Int)

  case class ConsumerDelta(consumerId: String, topic: String, partition: Int, offset: Long, topicOffset: Option[Long], messagesLeft: Option[Long], lastModified: Option[Date])

  case class Inbound(topic: String, partition: Int, startOffset: Long, endOffset: Long, change: Long, msgsPerSec: Double, lastCheckTime: Date)

  sealed trait KafkaMessageCursor extends MessageCursor {

    def topic: String

    def partition: Int

    def offset: Long

    def decoder: Option[MessageDecoder[_]]

  }

  case class KafkaNavigableCursor(topic: String,
                                  partition: Int,
                                  offset: Long,
                                  nextOffset: Long,
                                  decoder: Option[MessageDecoder[_]]) extends KafkaMessageCursor

  case class KafkaWatchCursor(topic: String,
                              groupId: String,
                              partition: Int,
                              offset: Long,
                              consumer: KafkaMacroConsumer,
                              iterator: Iterator[StreamedMessage],
                              decoder: Option[MessageDecoder[_]]) extends KafkaMessageCursor

  case class MessageMaxMin(minimumSize: Int, maximumSize: Int)

  case class TopicAndGroup(topic: String, groupId: String)

  case class TopicItem(topic: String, partition: Int, leader: String, replicas: Int, inSync: Int)

  case class TopicItemCompact(topic: String, partitions: Int, replicated: String)

  case class TopicOffsets(topic: String, partition: Int, startOffset: Long, endOffset: Long, messagesAvailable: Long)

  case class WatchCursorItem(groupId: String, topic: String, partition: Int, offset: Long, decoder: Option[MessageDecoder[_]])

}