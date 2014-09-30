package com.ldaniels528.trifecta.modules.kafka

import java.util.Date
import java.util.concurrent.atomic.AtomicLong

import com.ldaniels528.trifecta.TxRuntimeContext
import com.ldaniels528.trifecta.command.UnixLikeArgs
import com.ldaniels528.trifecta.modules.kafka.KafkaFacade._
import com.ldaniels528.trifecta.support.avro.AvroDecoder
import com.ldaniels528.trifecta.support.io.{BinaryOutputHandler, MessageOutputHandler, OutputHandler}
import com.ldaniels528.trifecta.support.kafka.KafkaMicroConsumer._
import com.ldaniels528.trifecta.support.kafka.{Broker, KafkaMicroConsumer, KafkaPublisher}
import com.ldaniels528.trifecta.support.messaging.logic.ConditionCompiler._
import com.ldaniels528.trifecta.support.messaging.{MessageCursor, MessageDecoder}
import com.ldaniels528.trifecta.support.messaging.logic.Condition
import com.ldaniels528.trifecta.support.zookeeper.ZKProxy
import com.ldaniels528.trifecta.util.TxUtils._
import kafka.common.TopicAndPartition

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

/**
 * Apache Kafka Facade
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class KafkaFacade(correlationId: Int) {

  /**
   * Returns a collection of brokers
   * @param zk the given [[ZKProxy]] instance
   * @return a collection of brokers
   */
  def brokers(implicit zk: ZKProxy): Seq[Broker] = {
    KafkaMicroConsumer.getBrokerList(zk) map (b => Broker(b.host, b.port))
  }

  /**
   * Commits the offset for a given topic and group ID
   */
  def commitOffset(topic: String, partition: Int, groupId: String, offset: Long, metadata: Option[String] = None)(implicit zk: ZKProxy) {
    new KafkaMicroConsumer(TopicAndPartition(topic, partition), brokers, correlationId) use (
      _.commitOffsets(groupId, offset, metadata getOrElse "N/A"))
  }

  /**
   * Counts the messages matching a given condition [references cursor]
   */
  def countMessages(topic: String, conditions: Seq[Condition], decoder: Option[MessageDecoder[_]])(implicit zk: ZKProxy, ec: ExecutionContext): Future[Long] = {
    KafkaMicroConsumer.count(topic, brokers, correlationId, conditions: _*)
  }

  /**
   * Returns the offsets for a given topic and group ID
   */
  def fetchOffsets(topic: String, partition: Int, groupId: String)(implicit zk: ZKProxy): Option[Long] = {
    new KafkaMicroConsumer(TopicAndPartition(topic, partition), brokers, correlationId) use (_.fetchOffsets(groupId))
  }

  /**
   * Finds messages that corresponds to the given criteria and exports them to a topic
   * @example kfind frequency > 5000 -o topic:highFrequency.quotes
   */
  def findMessages(topic: String, decoder: Option[MessageDecoder[_]], conditions: Seq[Condition], outputHandler: OutputHandler)(implicit zk: ZKProxy, ec: ExecutionContext): Future[Long] = {
    // define the counter
    val counter = new AtomicLong(0L)

    // find and export the messages matching our criteria
    val task = KafkaMicroConsumer.observe(topic, brokers, correlationId) { md =>
      if (conditions.forall(_.satisfies(md.message, md.key))) {
        outputHandler match {
          case device: MessageOutputHandler => device.write(decoder, md.key, md.message)
          case device: BinaryOutputHandler => device.write(md.key, md.message)
          case device => dieNoOutputHandler(device)
        }
        counter.incrementAndGet()
        ()
      }
    }

    // upon completion, close the output device and return the count
    task.map { u =>
      outputHandler.close()
      counter.get
    }
  }

  /**
   * Retrieves the list of Kafka consumers
   */
  def getConsumers(consumerPrefix: Option[String], topicPrefix: Option[String])(implicit zk: ZKProxy, ec: ExecutionContext): Future[List[ConsumerDelta]] = {
    // get the Kafka consumer groups
    val consumersCG = Future {
      KafkaMicroConsumer.getConsumerList(topicPrefix) map { c =>
        val topicOffset = getLastOffset(c.topic, c.partition)
        val delta = topicOffset map (offset => Math.max(0L, offset - c.offset))
        ConsumerDelta(c.consumerId, c.topic, c.partition, c.offset, topicOffset, delta)
      }
    }

    // get the Kafka Spout consumers (Partition Manager)
    val consumersPM = Future {
      KafkaMicroConsumer.getSpoutConsumerList() map { c =>
        val topicOffset = getLastOffset(c.topic, c.partition)
        val delta = topicOffset map (offset => Math.max(0L, offset - c.offset))
        ConsumerDelta(c.topologyName, c.topic, c.partition, c.offset, topicOffset, delta)
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
    new KafkaMicroConsumer(TopicAndPartition(topic, partition), brokers, correlationId) use (_.getFirstOffset)
  }

  /**
   * Returns the last offset for a given topic
   */
  def getLastOffset(topic: String, partition: Int)(implicit zk: ZKProxy): Option[Long] = {
    new KafkaMicroConsumer(TopicAndPartition(topic, partition), brokers, correlationId) use (_.getLastOffset)
  }

  /**
   * Retrieves either a binary or decoded message
   * @param topic the given topic
   * @param partition the given partition
   * @param offset the given offset
   * @param fetchSize the given fetch size
   * @return either a binary or decoded message
   */
  def getMessage(topic: String, partition: Int, offset: Long,
                 instant: Option[Long],
                 decoder: Option[MessageDecoder[_]],
                 outputHandler: Option[OutputHandler],
                 fetchSize: Int)(implicit zk: ZKProxy, rt: TxRuntimeContext, ec: ExecutionContext): Either[Option[MessageData], Seq[AvroRecord]] = {
    // retrieve the message
    val messageData = new KafkaMicroConsumer(TopicAndPartition(topic, partition), brokers, correlationId) use { consumer =>
      val myOffset: Long = instant flatMap (t => consumer.getOffsetsBefore(t).headOption) getOrElse offset
      consumer.fetch(myOffset, fetchSize).headOption
    }

    // if a decoder was found, use it to decode the message
    val decodedMessage = decoder.flatMap(decodeMessage(messageData, _))

    // write the message to an output handler
    messageData.foreach(md => handleOutputFlag(outputHandler, decoder, md.key, md.message))

    // return either a binary message or a decoded message
    decodedMessage.map(Right(_)) getOrElse Left(messageData)
  }

  private def handleOutputFlag(outputHandler: Option[OutputHandler], decoder: Option[MessageDecoder[_]], key: Array[Byte], message: Array[Byte])(implicit ec: ExecutionContext) = {
    outputHandler match {
      case Some(device: MessageOutputHandler) => device use (_.write(decoder, key, message))
      case Some(device: BinaryOutputHandler) => device use (_.write(key, message))
      case Some(unhandled) => dieNoOutputHandler(unhandled)
      case None => die("No such output device")
    }
  }

  /**
   * Decodes the given message
   * @param messageData the given option of a message
   * @param aDecoder the given message decoder
   * @return the decoded message
   */
  private def decodeMessage(messageData: Option[MessageData], aDecoder: MessageDecoder[_]): Option[Seq[AvroRecord]] = {
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
    new KafkaMicroConsumer(TopicAndPartition(topic, partition), brokers, correlationId) use { consumer =>
      consumer.fetch(offset, fetchSize).headOption map (_.key)
    }
  }

  /**
   * Returns the size of the message for a given topic partition and offset
   */
  def getMessageSize(topic: String, partition: Int, offset: Long, fetchSize: Int)(implicit zk: ZKProxy): Option[Int] = {
    new KafkaMicroConsumer(TopicAndPartition(topic, partition), brokers, correlationId) use {
      _.fetch(offset.toLong, fetchSize).headOption map (_.message.length)
    }
  }

  /**
   * Returns the minimum and maximum message size for a given topic partition and offset range
   */
  def getMessageMinMaxSize(topic: String, partition: Int, startOffset: Long, endOffset: Long, fetchSize: Int)(implicit zk: ZKProxy): Seq[MessageMaxMin] = {
    new KafkaMicroConsumer(TopicAndPartition(topic, partition), brokers, correlationId) use { consumer =>
      val offsets = startOffset.toLong to endOffset.toLong
      val messages = consumer.fetch(offsets, fetchSize).map(_.message.length)
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
    val topicData = KafkaMicroConsumer.getTopicList(brokers, correlationId)

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
   * "kpublish" - Returns the EOF offset for a given topic
   */
  def publishMessage(topic: String, key: Array[Byte], message: Array[Byte])(implicit zk: ZKProxy): Unit = {
    KafkaPublisher(brokers) use { publisher =>
      publisher.open()
      publisher.publish(topic, key, message)
    }
  }

  /**
   * Sets the offset of a consumer group ID to zero for all partitions
   */
  def resetConsumerGroup(topic: String, groupId: String)(implicit zk: ZKProxy): Unit = {
    // get the partition range
    val partitions = KafkaMicroConsumer.getTopicList(brokers, correlationId) filter (_.topic == topic) map (_.partitionId)
    if (partitions.isEmpty)
      throw new IllegalStateException(s"No partitions found for topic $topic")
    val (start, end) = (partitions.min, partitions.max)

    // reset the consumer group ID for each partition
    (start to end) foreach { partition =>
      new KafkaMicroConsumer(TopicAndPartition(topic, partition), brokers, correlationId = 0) use { consumer =>
        consumer.commitOffsets(groupId, offset = 0L, "resetting consumer ID")
      }
    }
  }

  protected def die[S](message: String): S = throw new IllegalArgumentException(message)

  private def dieNoCursor[S](): S = die("No topic/partition specified and no cursor exists")

  private def dieNoInputSource[S](): S = die("No input source specified")

  private def dieNoOutputSource[S](): S = die("No output source specified")

  private def dieNoOutputHandler(device: OutputHandler) = die(s"Unhandled output device $device")

  private def dieNotMessageComparator[S](): S = die("Decoder does not support logical operations")

}

/**
 * Kafka Facade Singleton
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
object KafkaFacade {

  case class AvroRecord(field: String, value: Any, `type`: String)

  case class AvroVerification(verified: Int, failed: Int)

  case class ConsumerDelta(consumerId: String, topic: String, partition: Int, offset: Long, topicOffset: Option[Long], messagesLeft: Option[Long])

  case class Inbound(topic: String, partition: Int, startOffset: Long, endOffset: Long, change: Long, msgsPerSec: Double, lastCheckTime: Date)

  case class KafkaCursor(topic: String, partition: Int, offset: Long, nextOffset: Long, decoder: Option[MessageDecoder[_]]) extends MessageCursor

  case class MessageMaxMin(minimumSize: Int, maximumSize: Int)

  case class TopicItem(topic: String, partition: Int, leader: String, replicas: Int, inSync: Int)

  case class TopicItemCompact(topic: String, partitions: Int, replicated: String)

  case class TopicOffsets(topic: String, partition: Int, startOffset: Long, endOffset: Long, messagesAvailable: Long)

}