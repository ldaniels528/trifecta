package com.ldaniels528.verify.support.kafka

import java.nio.ByteBuffer
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}

import com.ldaniels528.verify.support.kafka.KafkaSubscriber._
import com.ldaniels528.verify.support.messaging.logic.Condition
import com.ldaniels528.verify.support.zookeeper.ZKProxy
import com.ldaniels528.verify.util.VxUtils._
import kafka.api._
import kafka.common._
import kafka.consumer.SimpleConsumer
import org.slf4j.LoggerFactory

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success, Try}

/**
 * Low-Level Kafka Consumer
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class KafkaSubscriber(topic: TopicSlice, seedBrokers: Seq[Broker], correlationId: Int) {
  // generate the client ID
  private val clientID = s"Client_${topic.name}_${topic.partition}_${System.currentTimeMillis()}"

  // get the leader, meta data and replica brokers
  private val (leader, _, replicas) = getLeaderPartitionMetaDataAndReplicas(topic, seedBrokers, correlationId)
    .getOrElse(throw new IllegalStateException(s"The leader broker could not be determined for $topic"))

  // get the initial broker (topic leader)
  private val broker: Broker = leader

  // get the connection (topic consumer)
  private val consumer: SimpleConsumer = connect(broker, clientID)

  /**
   * Closes the underlying consumer instance
   */
  def close(): Unit = consumer.close()

  /**
   * Commits an offset for the given consumer group
   */
  def commitOffsets(groupId: String, offset: Long, metadata: String): Option[Short] = {
    // create the topic/partition and request information
    val topicAndPartition = new TopicAndPartition(topic.name, topic.partition)
    val requestInfo = Map(topicAndPartition -> new OffsetMetadataAndError((offset, metadata, 1.toShort)))

    // submit the request, and retrieve the response
    val request = new OffsetCommitRequest(groupId, requestInfo, OffsetRequest.CurrentVersion, correlationId, clientID)
    val response = consumer.commitOffsets(request)

    // retrieve the response code
    for {
      topicMap <- response.requestInfoGroupedByTopic.get(topic.name)
      code <- topicMap.get(new TopicAndPartition(topic.name, topic.partition))
    } yield code
  }

  /**
   * Returns the earliest or latest offset for the given consumer ID
   * @param consumerId the given consumer ID
   * @param timeInMillis the given time in milliseconds
   * @return the earliest or latest offset
   */
  def earliestOrLatestOffset(consumerId: Int, timeInMillis: Long): Option[Long] = {
    val topicAndPartition = new TopicAndPartition(topic.name, topic.partition)
    Option(consumer.earliestOrLatestOffset(topicAndPartition, timeInMillis, consumerId))
  }

  /**
   * Retrieves the message for the given corresponding offset
   * @param offset the given offset
   * @param fetchSize the fetch size
   * @return the response messages
   */
  def fetch(offset: Long, fetchSize: Int): Iterable[MessageData] = fetch(Seq(offset), fetchSize)

  /**
   * Retrieves messages for the given corresponding offsets
   * @param offsets the given offsets
   * @param fetchSize the fetch size
   * @return the response messages
   */
  def fetch(offsets: Seq[Long], fetchSize: Int): Iterable[MessageData] = {
    // build the request
    val request = offsets.foldLeft(new FetchRequestBuilder().clientId(clientID)) {
      (builder, offset) =>
        builder.addFetch(topic.name, topic.partition, offset, fetchSize)
        builder
    }.build()

    // submit the request, and process the response
    val response = consumer.fetch(request)
    if (response.hasError) throw new KafkaFetchException(response.errorCode(topic.name, topic.partition))
    else {
      val lastOffset = response.highWatermark(topic.name, topic.partition)
      response.messageSet(topic.name, topic.partition) map { msgAndOffset =>
        val key = Option(msgAndOffset.message) map (_.key) getOrElse ByteBuffer.allocate(0)
        val payload = Option(msgAndOffset.message) map (_.payload) getOrElse ByteBuffer.allocate(0)
        MessageData(msgAndOffset.offset, msgAndOffset.nextOffset, lastOffset, toArray(key), toArray(payload))
      }
    }
  }

  /**
   * Retrieves the offset of a consumer group ID
   * @param groupId the given consumer group ID (e.g. myConsumerGroup)
   * @return an option of an offset
   */
  def fetchOffsets(groupId: String): Option[Long] = {
    // create the topic/partition and request information
    val topicAndPartition = new TopicAndPartition(topic.name, topic.partition)
    val requestInfo = Seq(topicAndPartition)

    // submit the request, and retrieve the response
    val request = new OffsetFetchRequest(groupId, requestInfo, OffsetFetchRequest.CurrentVersion, correlationId, clientID)
    val response = consumer.fetchOffsets(request)

    // retrieve the offset(s)
    for {
      topicMap <- response.requestInfoGroupedByTopic.get(topic.name)
      ome <- topicMap.get(topicAndPartition)
    } yield ome.offset
  }

  /**
   * Returns the first available offset
   * @return an option of an offset
   */
  def getFirstOffset: Option[Long] = getOffsetsBefore(OffsetRequest.EarliestTime)

  /**
   * Returns the last available offset
   * @return an option of an offset
   */
  def getLastOffset: Option[Long] = getOffsetsBefore(OffsetRequest.LatestTime) map (offset => Math.max(0, offset - 1))

  /**
   * Returns the offset for an instance in time
   * @param time the given time EPOC in milliseconds
   * @return an option of an offset
   */
  def getOffsetsBefore(time: Long): Option[Long] = {
    // create the topic/partition and request information
    val topicAndPartition = new TopicAndPartition(topic.name, topic.partition)
    val requestInfo = Map(topicAndPartition -> new PartitionOffsetRequestInfo(time, 1))
    val replicaId = replicas.indexOf(broker)

    // submit the request, and retrieve the response
    val request = new OffsetRequest(requestInfo, correlationId, replicaId)
    val response = consumer.getOffsetsBefore(request)
    //logger.info(s"response = $response, offsetsGroupedByTopic = ${response.offsetsGroupedByTopic.get(topic.name)}")

    // handle the response
    if (response.hasError) {
      response.partitionErrorAndOffsets map {
        case (tap, por) =>
          val code = por.error
          throw new RuntimeException(s"Error fetching data Offset Data the Broker. Reason: $code - ${ERROR_CODES.getOrElse(code, s"UNKNOWN - $code")}")
      }
      None
    } else {
      // return the first offset
      for {
        topicMap <- response.offsetsGroupedByTopic.get(topic.name)
        por <- topicMap.get(topicAndPartition)
        offset <- por.offsets.headOption
      } yield offset
    }
  }

  private def toArray(payload: ByteBuffer): Array[Byte] = {
    val bytes = new Array[Byte](payload.limit)
    payload.get(bytes)
    bytes
  }

  private def findNewLeader(oldLeader: Broker, correlationId: Int): Option[Broker] = {
    def f() = for {
      (leader, metadata, replicas) <- getLeaderPartitionMetaDataAndReplicas(topic, replicas, correlationId)
    } yield leader
    untilTimeout(5.seconds, 1.second, f)
  }

  private def untilTimeout[T](duration: FiniteDuration, delay: FiniteDuration, f: () => Option[T]) = {
    val startTime = System.currentTimeMillis
    var result: Option[T] = None
    while (result.isEmpty && (System.currentTimeMillis - startTime < duration)) {
      result = f()
      if (result.isEmpty) {
        Thread.sleep(delay)
      }
    }
    result
  }

}

/**
 * Verify Kafka Message Subscriber Singleton
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
object KafkaSubscriber {
  private lazy val logger = LoggerFactory.getLogger(getClass)

  // setup defaults
  private val DEFAULT_FETCH_SIZE: Int = 65536

  /**
   * Returns the promise of the total number of a messages that match the given search criteria
   * @param topic the given topic name
   * @param brokers the given replica brokers
   * @param correlationId the given correlation ID
   * @param conditions the given search criteria
   * @return the promise of the total number of a messages that the given search criteria
   */
  def count(topic: String, brokers: Seq[Broker], correlationId: Int, conditions: Condition*)(implicit ec: ExecutionContext, zk: ZKProxy): Future[Long] = {
    val promise = Promise[Long]()
    val counter = new AtomicLong(0)
    val tasks = getTopicPartitions(topic) map { partition =>
      Future {
        new KafkaSubscriber(TopicSlice(topic, partition), brokers, correlationId) use { subs =>
          var offset: Option[Long] = subs.getFirstOffset
          val lastOffset: Option[Long] = subs.getLastOffset
          def eof: Boolean = offset.exists(o => lastOffset.exists(o > _))
          while (!eof) {
            for {
              ofs <- offset
              msg <- subs.fetch(ofs, DEFAULT_FETCH_SIZE).headOption
            } if (conditions.forall(_.satisfies(msg.message, msg.key))) counter.incrementAndGet()
            offset = offset map (_ + 1)
          }
        }
      }
    }

    // check for the failure to complete the count
    Future.sequence(tasks).onComplete {
      case Success(v) => promise.success(counter.get)
      case Failure(e) => promise.failure(e)
    }
    promise.future
  }

  /**
   * Returns the promise of the option of a message based on the given search criteria
   * @param topic the given topic name
   * @param brokers the given replica brokers
   * @param correlationId the given correlation ID
   * @param conditions the given search criteria
   * @return the promise of the option of a message based on the given search criteria
   */
  def findOne(topic: String, brokers: Seq[Broker], correlationId: Int, conditions: Condition*)(implicit ec: ExecutionContext, zk: ZKProxy): Future[Option[(Int, MessageData)]] = {
    val promise = Promise[Option[(Int, MessageData)]]()
    val found = new AtomicBoolean(false)
    var message: Option[(Int, MessageData)] = None
    val tasks = getTopicPartitions(topic) map { partition =>
      Future {
        new KafkaSubscriber(TopicSlice(topic, partition), brokers, correlationId) use { subs =>
          var offset: Option[Long] = subs.getFirstOffset
          val lastOffset: Option[Long] = subs.getLastOffset
          def eof: Boolean = offset.exists(o => lastOffset.exists(o > _))
          while (!found.get && !eof) {
            for {
              ofs <- offset
              msg <- subs.fetch(ofs, DEFAULT_FETCH_SIZE).headOption
            } {
              if (conditions.forall(_.satisfies(msg.message, msg.key)) && found.compareAndSet(false, true)) message = Option((partition, msg))
            }

            offset = offset map (_ + 1)
          }
        }
      }
    }

    // check for the failure to find a message
    Future.sequence(tasks).onComplete {
      case Success(v) => promise.success(message)
      case Failure(e) => promise.failure(e)
    }
    promise.future
  }

  /**
   * Retrieves the list of defined brokers from Zookeeper
   */
  def getBrokerList(implicit zk: ZKProxy): Seq[BrokerDetails] = {
    import net.liftweb.json._
    implicit val formats = DefaultFormats
    val sdf = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss z")
    val basePath = "/brokers/ids"
    zk.getChildren(basePath) flatMap { brokerId =>
      zk.readString(s"$basePath/$brokerId") map { json =>
        val details = parse(json).extract[BrokerDetails]
        Try(details.timestamp = sdf.format(new java.util.Date(details.timestamp.toLong)))
        details
      }
    }
  }

  /**
   * Retrieves the list of consumers from Zookeeper
   */
  def getConsumerList(topicPrefix: Option[String] = None)(implicit zk: ZKProxy): Seq[ConsumerDetails] = {
    // start with the list of consumer IDs
    val basePath = "/consumers"
    zk.getChildren(basePath) flatMap { consumerId =>
      // get the list of topics
      val offsetPath = s"$basePath/$consumerId/offsets"
      val topics = zk.getChildren(offsetPath).distinct filter (t => topicPrefix.isEmpty || topicPrefix.exists(t.startsWith))

      // get the list of partitions
      topics flatMap { topic =>
        val topicPath = s"$offsetPath/$topic"
        val partitions = zk.getChildren(topicPath)
        partitions flatMap { partitionId =>
          zk.readString(s"$topicPath/$partitionId") map (offset => ConsumerDetails(consumerId, topic, partitionId.toInt, offset.toLong))
        }
      }
    }
  }

  case class ConsumerDetails(consumerId: String, topic: String, partition: Int, offset: Long)

  /**
   * Returns the list of partitions for the given topic
   */
  def getTopicPartitions(topic: String)(implicit zk: ZKProxy): Seq[Int] = {
    zk.getChildren(path = s"/brokers/topics/$topic/partitions") map (_.toInt)
  }

  /**
   * Returns the list of topics for the given brokers
   */
  def getTopicList(brokers: Seq[Broker], correlationId: Int)(implicit zk: ZKProxy): Seq[TopicDetails] = {
    // get the list of topics
    val topics = zk.getChildren(path = "/brokers/topics")

    // capture the meta data for all topics
    getTopicMetadata(brokers.head, topics, correlationId) flatMap { tmd =>
      // check for errors
      if (tmd.errorCode != 0) throw new KafkaFetchException(tmd.errorCode)

      // translate the partition meta data into topic information instances
      tmd.partitionsMetadata map { pmd =>
        // check for errors
        if (pmd.errorCode != 0) throw new KafkaFetchException(pmd.errorCode)

        TopicDetails(
          tmd.topic,
          pmd.partitionId,
          pmd.leader map (b => Broker(b.host, b.port, b.id)),
          pmd.replicas map (b => Broker(b.host, b.port, b.id)),
          pmd.isr map (b => Broker(b.host, b.port)),
          tmd.sizeInBytes)
      }
    }
  }

  /**
   * Establishes a connection with the specified broker
   * @param broker the specified { @link Broker broker}
   */
  private def connect(broker: Broker, clientID: String): SimpleConsumer = {
    new SimpleConsumer(broker.host, broker.port, DEFAULT_FETCH_SIZE, 63356, clientID)
  }

  /**
   * Retrieves the partition meta data and replicas for the lead broker
   */
  private def getLeaderPartitionMetaDataAndReplicas(topic: TopicSlice, brokers: Seq[Broker], correlationId: Int): Option[(Broker, PartitionMetadata, Seq[Broker])] = {
    for {
      pmd <- brokers.foldLeft[Option[PartitionMetadata]](None)((result, broker) => result ?? getPartitionMetadata(broker, topic, correlationId))
      leader <- pmd.leader map (r => Broker(r.host, r.port))
      replicas = pmd.replicas map (r => Broker(r.host, r.port))
    } yield (leader, pmd, replicas)
  }

  /**
   * Retrieves the partition meta data for the given broker
   */
  private def getPartitionMetadata(broker: Broker, topic: TopicSlice, correlationId: Int): Option[PartitionMetadata] = {
    connect(broker, clientID = s"pmdLookup_${System.currentTimeMillis()}") use { consumer =>
      Try {
        // submit the request and retrieve the response
        val response = consumer.send(new TopicMetadataRequest(Seq(topic.name), correlationId))

        // capture the meta data for the partition
        (response.topicsMetadata flatMap { tmd =>
          tmd.partitionsMetadata.find(m => m.partitionId == topic.partition)
        }).headOption

      } match {
        case Success(pmd) => pmd
        case Failure(e) =>
          throw new RuntimeException(s"Error communicating with Broker [$broker] to find Leader for [$topic] Reason: ${e.getMessage}")
      }
    }
  }

  /**
   * Retrieves the partition meta data for the given broker
   */
  private def getTopicMetadata(broker: Broker, topics: Seq[String], correlationId: Int): Seq[TopicMetadata] = {
    connect(broker, clientID = s"tmdLookup_${System.currentTimeMillis()}") use { consumer =>
      Try {
        // submit the request and retrieve the response
        val response = consumer.send(new TopicMetadataRequest(topics, correlationId))

        // capture the meta data for the partition
        response.topicsMetadata

      } match {
        case Success(tmds) => tmds
        case Failure(e) =>
          e.printStackTrace()
          throw new RuntimeException(s"Error communicating with Broker [$broker] Reason: ${e.getMessage}")
      }
    }
  }

  /**
   * Returns the promise of the option of a message based on the given search criteria
   * @param topic the given topic name
   * @param brokers the given replica brokers
   * @param correlationId the given correlation ID
   * @param observer the given callback function
   * @return the promise of the option of a message based on the given search criteria
   */
  def observe(topic: String, brokers: Seq[Broker], correlationId: Int)(observer: MessageData => Unit)(implicit ec: ExecutionContext, zk: ZKProxy): Future[Seq[Unit]] = {
    Future.sequence(getTopicPartitions(topic) map { partition =>
      Future {
        new KafkaSubscriber(TopicSlice(topic, partition), brokers, correlationId) use { subs =>
          var offset: Option[Long] = subs.getFirstOffset
          val lastOffset: Option[Long] = subs.getLastOffset
          def eof: Boolean = offset.exists(o => lastOffset.exists(o > _))
          while (!eof) {
            for (ofs <- offset; msg <- subs.fetch(ofs, DEFAULT_FETCH_SIZE).headOption) observer(msg)
            offset = offset map (_ + 1)
          }
        }
      }
    })
  }

  /**
   * Represents a message and offset
   * @param offset the offset of the message within the topic partition
   * @param nextOffset the next available offset
   * @param message the message
   */
  case class MessageData(offset: Long, nextOffset: Long, lastOffset: Long, key: Array[Byte], message: Array[Byte])

  /**
   * Represents the details for a Kafka topic
   */
  case class TopicDetails(topic: String, partitionId: Int, leader: Option[Broker], replicas: Seq[Broker], isr: Seq[Broker], sizeInBytes: Int)

  /**
   * Object representation of the broker information JSON
   * {"jmx_port":9999,"timestamp":"1405818758964","host":"vsccrtc204-brn1.rtc.vrsn.com","version":1,"port":9092}
   */
  case class BrokerDetails(jmx_port: Int, var timestamp: String, host: String, version: Int, port: Int)

  /**
   * Represents a class of exceptions that occur while attempting to fetch data from a Kafka broker
   * @param code the status/error code
   */
  class KafkaFetchException(val code: Short)
    extends RuntimeException(ERROR_CODES.getOrElse(code, "Unrecognized Error Code"))

  /**
   * Kafka Error Codes
   * @see https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol
   */

  import kafka.common.ErrorMapping._

  val ERROR_CODES = Map(
    BrokerNotAvailableCode -> "Broker Not Available",
    InvalidFetchSizeCode -> "Invalid Fetch Size",
    InvalidMessageCode -> "Invalid Message",
    LeaderNotAvailableCode -> "Leader Not Available",
    MessageSizeTooLargeCode -> "Message Size Too Large",
    NoError -> "No Error",
    NotLeaderForPartitionCode -> "Not Leader For Partition",
    OffsetMetadataTooLargeCode -> "Offset Metadata Too Large",
    OffsetOutOfRangeCode -> "Offset Out Of Range",
    ReplicaNotAvailableCode -> "Replica Not Available",
    RequestTimedOutCode -> "Request Timed Out",
    StaleControllerEpochCode -> "Stale Controller Epoch",
    StaleLeaderEpochCode -> "Stale Leader Epoch",
    UnknownCode -> "Unknown Code",
    UnknownTopicOrPartitionCode -> "Unknown Topic-Or-Partition")

}
