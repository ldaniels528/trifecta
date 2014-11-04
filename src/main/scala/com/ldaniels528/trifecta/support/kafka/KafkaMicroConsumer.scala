package com.ldaniels528.trifecta.support.kafka

import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}

import com.ldaniels528.trifecta.support.kafka.KafkaMicroConsumer._
import com.ldaniels528.trifecta.support.messaging.BinaryMessage
import com.ldaniels528.trifecta.support.messaging.logic.Condition
import com.ldaniels528.trifecta.support.zookeeper.ZKProxy
import com.ldaniels528.trifecta.util.ByteBufferUtils._
import com.ldaniels528.trifecta.util.TxUtils._
import kafka.api._
import kafka.common._
import kafka.consumer.SimpleConsumer
import net.liftweb.json._

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success, Try}

/**
 * Kafka Low-Level Message Consumer
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class KafkaMicroConsumer(topicAndPartition: TopicAndPartition, seedBrokers: Seq[Broker], correlationId: Int) {
  // get the leader, meta data and replica brokers
  private val (broker, _, replicas) = getLeaderPartitionMetaDataAndReplicas(topicAndPartition, seedBrokers, correlationId)
    .getOrElse(throw new VxKafkaTopicException("The leader broker could not be determined", topicAndPartition))

  // generate the client ID
  private val clientID = makeClientID("consumer")

  // get the connection (topic consumer)
  private val consumer: SimpleConsumer = connect(broker, clientID)

  /**
   * Closes the underlying consumer instance
   */
  def close(): Unit = consumer.close()

  /**
   * Commits an offset for the given consumer group
   */
  def commitOffsets(groupId: String, offset: Long, metadata: String) {
    // create the topic/partition and request information
    val requestInfo = Map(topicAndPartition ->  OffsetAndMetadata(offset, metadata, timestamp = System.currentTimeMillis()))

    // submit the request, and retrieve the response
    val request = OffsetCommitRequest(groupId, requestInfo, OffsetRequest.CurrentVersion, correlationId, clientID)
    val response = consumer.commitOffsets(request)

    // retrieve the response code
    for {
      topicMap <- response.commitStatusGroupedByTopic.get(topicAndPartition.topic)
      code <- topicMap.get(topicAndPartition)
    } if (code != 0) {
      throw new VxKafkaCodeException(code)
    }
  }

  /**
   * Returns the earliest or latest offset for the given consumer ID
   * @param consumerId the given consumer ID
   * @param timeInMillis the given time in milliseconds
   * @return the earliest or latest offset
   */
  def earliestOrLatestOffset(consumerId: Int, timeInMillis: Long): Option[Long] = {
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
        builder.addFetch(topicAndPartition.topic, topicAndPartition.partition, offset, fetchSize)
        builder
    }.build()

    // submit the request, and process the response
    val response = consumer.fetch(request)
    if (response.hasError) throw new VxKafkaCodeException(response.errorCode(topicAndPartition.topic, topicAndPartition.partition))
    else {
      val lastOffset = response.highWatermark(topicAndPartition.topic, topicAndPartition.partition)
      response.messageSet(topicAndPartition.topic, topicAndPartition.partition) map { msgAndOffset =>
        val key: Array[Byte] = Option(msgAndOffset.message) map (_.key) map toArray getOrElse Array.empty
        val message: Array[Byte] = Option(msgAndOffset.message) map (_.payload) map toArray getOrElse Array.empty
        MessageData(msgAndOffset.offset, msgAndOffset.nextOffset, lastOffset, key, message)
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
    val requestInfo = Seq(topicAndPartition)

    // submit the request, and retrieve the response
    val request = new OffsetFetchRequest(groupId, requestInfo, OffsetFetchRequest.CurrentVersion, correlationId, clientID)
    val response = consumer.fetchOffsets(request)

    // retrieve the offset(s)
    for {
      topicMap <- response.requestInfoGroupedByTopic.get(topicAndPartition.topic)
      ome <- topicMap.get(topicAndPartition)
    } yield ome.offset
  }

  /**
   * Returns the first available offset
   * @return an option of an offset
   */
  def getFirstOffset: Option[Long] = getOffsetsBefore(OffsetRequest.EarliestTime).headOption

  /**
   * Returns the last available offset
   * @return an option of an offset
   */
  def getLastOffset: Option[Long] = getOffsetsBefore(OffsetRequest.LatestTime).headOption map (offset => Math.max(0, offset - 1))

  /**
   * Returns the offset for an instance in time
   * @param time the given time EPOC in milliseconds
   * @return an option of an offset
   */
  def getOffsetsBefore(time: Long): Seq[Long] = {
    // create the topic/partition and request information
    val requestInfo = Map(topicAndPartition -> new PartitionOffsetRequestInfo(time, 1))
    val replicaId = replicas.indexOf(broker)

    // submit the request, and retrieve the response
    val request = new OffsetRequest(requestInfo, correlationId, replicaId)
    val response = consumer.getOffsetsBefore(request)
    //logger.info(s"response = $response, offsetsGroupedByTopic = ${response.offsetsGroupedByTopic.get(topic.name)}")

    // handle the response
    if (response.hasError) {
      response.partitionErrorAndOffsets map {
        case (tap, por) => throw new VxKafkaCodeException(por.error)
      }
      Nil
    } else (for {
      topicMap <- response.offsetsGroupedByTopic.get(topicAndPartition.topic)
      por <- topicMap.get(topicAndPartition)
    } yield por.offsets) getOrElse Nil
  }

}

/**
 * Verify Kafka Message Subscriber Singleton
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
object KafkaMicroConsumer {
  private implicit val formats = net.liftweb.json.DefaultFormats
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
        new KafkaMicroConsumer(TopicAndPartition(topic, partition), brokers, correlationId) use { subs =>
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
        new KafkaMicroConsumer(TopicAndPartition(topic, partition), brokers, correlationId) use { subs =>
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
   * Returns the promise of the option of a message based on the given search criteria
   * @param tap the given [[TopicAndPartition]]
   * @param brokers the given replica brokers
   * @param correlationId the given correlation ID
   * @param conditions the given search criteria
   * @return the promise of the option of a message based on the given search criteria
   */
  def findNext(tap: TopicAndPartition, brokers: Seq[Broker], correlationId: Int, conditions: Condition*)(implicit ec: ExecutionContext, zk: ZKProxy): Future[Option[MessageData]] = {
    Future {
      var message: Option[MessageData] = None
      new KafkaMicroConsumer(tap, brokers, correlationId) use { subs =>
        var offset: Option[Long] = subs.getFirstOffset
        val lastOffset: Option[Long] = subs.getLastOffset
        def eof: Boolean = offset.exists(o => lastOffset.exists(o > _))
        while (!message.isDefined && !eof) {
          for {
            ofs <- offset
            msg <- subs.fetch(ofs, DEFAULT_FETCH_SIZE).headOption
          } {
            if (conditions.forall(_.satisfies(msg.message, msg.key))) message = Option(msg)
          }

          offset = offset map (_ + 1)
        }
      }
      message
    }
  }

  /**
   * Retrieves the list of defined brokers from Zookeeper
   */
  def getBrokerList(implicit zk: ZKProxy): Seq[BrokerDetails] = {
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
    val basePath = "/consumers"

    // start with the list of consumer IDs
    zk.getChildren(basePath) flatMap { consumerId =>
      // get the list of topics
      val offsetPath = s"$basePath/$consumerId/offsets"
      val topics = zk.getChildren(offsetPath).distinct filter (contentFilter(topicPrefix, _))

      // get the list of partitions
      topics flatMap { topic =>
        val topicPath = s"$offsetPath/$topic"
        val partitions = zk.getChildren(topicPath)
        partitions flatMap { partitionId =>
          zk.readString(s"$topicPath/$partitionId") flatMap (offset =>
            successOnly(Try(ConsumerDetails(consumerId, topic, partitionId.toInt, offset.toLong))))
        }
      }
    }
  }

  /**
   * Retrieves the list of consumers from Zookeeper (Kafka Spout / Partition Manager Version)
   */
  def getSpoutConsumerList()(implicit zk: ZKProxy): Seq[ConsumerDetailsPM] = {
    zk.getFamily(path = "/").distinct filter (_.matches( """\S+[/]partition_\d+""")) flatMap { path =>
      zk.readString(path) flatMap { jsonString =>
        successOnly(Try {
          val json = parse(jsonString)
          val id = (json \ "topology" \ "id").extract[String]
          val name = (json \ "topology" \ "name").extract[String]
          val topic = (json \ "topic").extract[String]
          val offset = (json \ "offset").extract[Long]
          val partition = (json \ "partition").extract[Int]
          val brokerHost = (json \ "broker" \ "host").extract[String]
          val brokerPort = (json \ "broker" \ "port").extract[Int]
          ConsumerDetailsPM(id, name, topic, partition, offset, s"$brokerHost:$brokerPort")
        })
      }
    }
  }

  /**
   * Transforms the given result into a [[Some]] if successful or [[None]] if failure
   * @param result  the given result
   * @return an Option representing success or failure
   */
  private def successOnly[S](result: Try[S]): Option[S] = result match {
    case Success(v) => Option(v)
    case Failure(e) => None
  }

  /**
   * Convenience method for filtering content (consumers, topics, etc.) by a prefix
   * @param prefix the given prefix
   * @param entity the given entity to filter
   * @return true, if the topic starts with the topic prefix
   */
  def contentFilter(prefix: Option[String], entity: String): Boolean = {
    prefix.isEmpty || prefix.exists(entity.startsWith)
  }

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
      if (tmd.errorCode != 0) throw new VxKafkaCodeException(tmd.errorCode)

      // translate the partition meta data into topic information instances
      tmd.partitionsMetadata map { pmd =>
        // check for errors
        if (pmd.errorCode != 0) throw new VxKafkaCodeException(pmd.errorCode)

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
        new KafkaMicroConsumer(TopicAndPartition(topic, partition), brokers, correlationId) use { subs =>
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
   * Establishes a connection with the specified broker
   * @param broker the specified { @link Broker broker}
   */
  private def connect(broker: Broker, clientID: String): SimpleConsumer = {
    new SimpleConsumer(broker.host, broker.port, DEFAULT_FETCH_SIZE, 63356, clientID)
  }

  /**
   * Retrieves the partition meta data and replicas for the lead broker
   */
  private def getLeaderPartitionMetaDataAndReplicas(topic: TopicAndPartition, brokers: Seq[Broker], correlationId: Int): Option[(Broker, PartitionMetadata, Seq[Broker])] = {
    for {
      pmd <- brokers.foldLeft[Option[PartitionMetadata]](None)((result, broker) => result ?? getPartitionMetadata(broker, topic, correlationId))
      leader <- pmd.leader map (r => Broker(r.host, r.port))
      replicas = pmd.replicas map (r => Broker(r.host, r.port))
    } yield (leader, pmd, replicas)
  }

  /**
   * Retrieves the partition meta data for the given broker
   */
  private def getPartitionMetadata(broker: Broker, topicAndPartition: TopicAndPartition, correlationId: Int): Option[PartitionMetadata] = {
    connect(broker, makeClientID("pmdLookup")) use { consumer =>
      Try {
        // submit the request and retrieve the response
        val response = consumer.send(new TopicMetadataRequest(Seq(topicAndPartition.topic), correlationId))

        // capture the meta data for the partition
        (response.topicsMetadata flatMap { tmd =>
          tmd.partitionsMetadata.find(m => m.partitionId == topicAndPartition.partition)
        }).headOption

      } match {
        case Success(pmd) => pmd
        case Failure(e) =>
          throw new VxKafkaTopicException(s"Error communicating with Broker [$broker] to find Leader", topicAndPartition, e)
      }
    }
  }

  /**
   * Retrieves the partition meta data for the given broker
   */
  private def getTopicMetadata(broker: Broker, topics: Seq[String], correlationId: Int): Seq[TopicMetadata] = {
    connect(broker, makeClientID("tmdLookup")) use { consumer =>
      Try {
        // submit the request and retrieve the response
        val response = consumer.send(new TopicMetadataRequest(topics, correlationId))

        // capture the meta data for the partition
        response.topicsMetadata

      } match {
        case Success(tmds) => tmds
        case Failure(e) =>
          throw new VxKafkaException(s"Error communicating with Broker [$broker] Reason: ${e.getMessage}", e)
      }
    }
  }

  /**
   * Generates a unique client identifier
   * @param prefix the given prefix
   * @return a unique client identifier
   */
  private def makeClientID(prefix: String): String = s"$prefix${System.nanoTime()}"

  /**
   * Represents the consumer group details for a given topic partition
   */
  case class ConsumerDetails(consumerId: String, topic: String, partition: Int, offset: Long)

  /**
   * Represents the consumer group details for a given topic partition (Kafka Spout / Partition Manager)
   */
  case class ConsumerDetailsPM(topologyId: String, topologyName: String, topic: String, partition: Int, offset: Long, broker: String)

  /**
   * Represents a message and offset
   * @param offset the offset of the message within the topic partition
   * @param nextOffset the next available offset
   * @param message the message
   */
  case class MessageData(offset: Long, nextOffset: Long, lastOffset: Long, key: Array[Byte], message: Array[Byte])
    extends BinaryMessage

  /**
   * Represents the details for a Kafka topic
   */
  case class TopicDetails(topic: String, partitionId: Int, leader: Option[Broker], replicas: Seq[Broker], isr: Seq[Broker], sizeInBytes: Int)

  /**
   * Object representation of the broker information JSON
   * {"jmx_port":9999,"timestamp":"1405818758964","host":"dev502","version":1,"port":9092}
   */
  case class BrokerDetails(jmx_port: Int, var timestamp: String, host: String, version: Int, port: Int)

  /**
   * Represents a class of exceptions that occur while attempting to fetch data from a Kafka broker
   * @param message the given error message
   * @param cause the given root cause of the exception
   */
  class VxKafkaException(message: String, cause: Throwable = null)
    extends RuntimeException(message, cause)

  /**
   * Represents a class of exceptions that occur while attempting to fetch data from a Kafka broker
   * @param code the status/error code
   */
  class VxKafkaCodeException(val code: Short)
    extends VxKafkaException(ERROR_CODES.getOrElse(code, "Unrecognized Error Code"))

  /**
   * Represents a class of exceptions that occur while consuming a Kafka message
   * @param message the given error message
   */
  class VxKafkaTopicException(message: String, tap: TopicAndPartition, cause: Throwable = null)
    extends VxKafkaException(s"$message for topic ${tap.topic} partition ${tap.partition}", cause)

  import kafka.common.ErrorMapping._

  /**
   * Kafka Error Codes
   * @see https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol
   */
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
