package com.ldaniels528.verify.modules.kafka

import com.ldaniels528.verify.modules.kafka.KafkaSubscriber._
import com.ldaniels528.verify.modules.zookeeper.ZKProxy
import com.ldaniels528.verify.util.VxUtils._
import kafka.api._
import kafka.common._
import kafka.consumer.SimpleConsumer
import kafka.message.MessageAndOffset

import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

/**
 * Kafka Subscriber
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class KafkaSubscriber(topic: Topic, seedBrokers: Seq[Broker], correlationId: Int) {
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
        MessageData(msgAndOffset.offset, msgAndOffset.nextOffset, lastOffset, getMessagePayload(msgAndOffset))
      }
    }
  }

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

  def getFirstOffset: Option[Long] = getOffsetsBefore(OffsetRequest.EarliestTime)

  def getLastOffset: Option[Long] = getOffsetsBefore(OffsetRequest.LatestTime) map (offset => Math.max(0, offset - 1))

  def getOffsetsBefore(time: Long): Option[Long] = {
    // create the topic/partition and request information
    val topicAndPartition = new TopicAndPartition(topic.name, topic.partition)
    val requestInfo = Map(topicAndPartition -> new PartitionOffsetRequestInfo(time, 1))
    val replicaId = 0

    // submit the request, and retrieve the response
    val request = new OffsetRequest(requestInfo, correlationId, replicaId)
    val response = consumer.getOffsetsBefore(request)

    // handle the response
    if (response.hasError) {
      response.partitionErrorAndOffsets map {
        case (tap, por) =>
          val code = por.error
          logger.error(s"Error fetching data Offset Data the Broker. Reason: $code - ${ERROR_CODES.getOrElse(code, s"UNKNOWN - $code")}")
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

  private def getMessagePayload(messageAndOffset: MessageAndOffset): Array[Byte] = {
    val payload = messageAndOffset.message.payload
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

  import net.liftweb.json._

  private val logger = org.slf4j.LoggerFactory.getLogger(getClass)

  // setup defaults
  private val DEFAULT_FETCH_SIZE: Int = 65536

  // define a date parser
  private val sdf = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss z")

  implicit val formats = DefaultFormats

  /**
   * Retrieves the list of defined brokers from Zookeeper
   */
  def getBrokerList(implicit zk: ZKProxy): Seq[BrokerDetails] = {
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
   * Returns the list of topics for the given brokers
   */
  def getTopicList(brokers: Seq[Broker], correlationId: Int)(implicit zk: ZKProxy): Seq[TopicDetails] = {
    // get the list of topics
    val topics = zk.getChildren("/brokers/topics")

    // capture the meta data for all topics
    getTopicMetadata(brokers(0), topics, correlationId) flatMap { tmd =>
      // check for errors
      if (tmd.errorCode != 0) throw new KafkaFetchException(tmd.errorCode)

      // translate the partition meta data into topic information instances
      tmd.partitionsMetadata map { pmd =>
        // check for errors
        if (pmd.errorCode != 0) throw new KafkaFetchException(pmd.errorCode)

        TopicDetails(
          tmd.topic,
          pmd.partitionId,
          pmd.leader map (b => Broker(b.host, b.port)),
          pmd.replicas map (b => Broker(b.host, b.port)),
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
  private def getLeaderPartitionMetaDataAndReplicas(topic: Topic, brokers: Seq[Broker], correlationId: Int): Option[(Broker, PartitionMetadata, Seq[Broker])] = {
    for {
      pmd <- brokers.foldLeft[Option[PartitionMetadata]](None)((result, broker) => result ?? getPartitionMetadata(broker, topic, correlationId))
      leader <- pmd.leader map (r => Broker(r.host, r.port))
      replicas = pmd.replicas map (r => Broker(r.host, r.port))
    } yield (leader, pmd, replicas)
  }

  /**
   * Retrieves the partition meta data for the given broker
   */
  private def getPartitionMetadata(broker: Broker, topic: Topic, correlationId: Int): Option[PartitionMetadata] = {
    connect(broker, s"pmdLookup_${System.currentTimeMillis()}") use { consumer =>
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
          logger.error(s"Error communicating with Broker [$broker] to find Leader for [$topic] Reason: ${e.getMessage}")
          None
      }
    }
  }

  /**
   * Retrieves the partition meta data for the given broker
   */
  private def getTopicMetadata(broker: Broker, topics: Seq[String], correlationId: Int): Seq[TopicMetadata] = {
    connect(broker, s"tmdLookup_${System.currentTimeMillis()}") use { consumer =>
      Try {
        // submit the request and retrieve the response
        val response = consumer.send(new TopicMetadataRequest(topics, correlationId))

        // capture the meta data for the partition
        response.topicsMetadata

      } match {
        case Success(tmds) => tmds
        case Failure(e) =>
          e.printStackTrace()
          logger.error(s"Error communicating with Broker [$broker] Reason: ${e.getMessage}")
          Seq.empty
      }
    }
  }

  /**
   * Represents a message and offset
   * @param offset the offset of the message within the topic partition
   * @param nextOffset the next available offset
   * @param message the message
   */
  case class MessageData(offset: Long, nextOffset: Long, lastOffset: Long, message: Array[Byte])

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
