package com.ldaniels528.trifecta.io.kafka

import java.util.concurrent.atomic.{AtomicInteger, AtomicLong, AtomicReference}

import com.ldaniels528.trifecta.io.AsyncIO.IOCounter
import com.ldaniels528.trifecta.io.ByteBufferUtils._
import com.ldaniels528.trifecta.io.kafka.KafkaMicroConsumer._
import com.ldaniels528.trifecta.io.zookeeper.ZKProxy
import com.ldaniels528.trifecta.messages.BinaryMessage
import com.ldaniels528.trifecta.messages.logic.Condition
import com.ldaniels528.commons.helpers.OptionHelper._
import com.ldaniels528.commons.helpers.ResourceHelper._
import kafka.api._
import kafka.common._
import kafka.consumer.SimpleConsumer
import net.liftweb.json._
import org.slf4j.LoggerFactory

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.language.postfixOps
import scala.util.{Failure, Success, Try}

/**
 * Kafka Low-Level Message Consumer
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class KafkaMicroConsumer(topicAndPartition: TopicAndPartition, seedBrokers: Seq[Broker]) {
  // get the leader, meta data and replica brokers
  val (leader, _, replicas) = getLeaderPartitionMetaDataAndReplicas(topicAndPartition, seedBrokers)
    .getOrElse(throw new VxKafkaTopicException("The leader broker could not be determined", topicAndPartition))

  // generate the client ID
  private val clientID = makeClientID("consumer")

  // get the connection (topic consumer)
  private val consumer = connect(leader, clientID)

  /**
   * Closes the underlying consumer instance
   */
  def close(): Unit = {
    Try(consumer.close())
    ()
  }

  /**
   * Commits an offset for the given consumer group
   */
  def commitOffsets(groupId: String, offset: Long, metadata: String) {
    // create the topic/partition and request information
    val requestInfo = Map(topicAndPartition -> OffsetAndMetadata(offset, metadata, timestamp = System.currentTimeMillis()))

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
   * Retrieves messages for the given corresponding offsets
   * @param offsets the given offsets
   * @param fetchSize the fetch size
   * @return the response messages
   */
  def fetch(offsets: Long*)(fetchSize: Int = 65536): Seq[MessageData] = {
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
      (response.messageSet(topicAndPartition.topic, topicAndPartition.partition) map { msgAndOffset =>
        val key: Array[Byte] = Option(msgAndOffset.message) map (_.key) map toArray getOrElse Array.empty
        val message: Array[Byte] = Option(msgAndOffset.message) map (_.payload) map toArray getOrElse Array.empty
        MessageData(topicAndPartition.partition, msgAndOffset.offset, msgAndOffset.nextOffset, lastOffset, key, message)
      }).toSeq
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
    val replicaId = replicas.indexOf(leader)

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
  private lazy val logger = LoggerFactory.getLogger(getClass)
  private implicit val formats = net.liftweb.json.DefaultFormats
  private val correlationIdGen = new AtomicInteger(-1)

  var rootKafkaPath: String = "/"
  val DEFAULT_FETCH_SIZE: Int = 65536

  /**
   * Returns the next correlation ID
   * @return the next correlation ID
   */
  def correlationId: Int = correlationIdGen.incrementAndGet()

  /**
   * Returns the promise of the total number of a messages that match the given search criteria
   * @param topic the given topic name
   * @param brokers the given replica brokers
   * @param conditions the given search criteria
   * @return the promise of the total number of messages that match the given search criteria
   */
  def count(topic: String, brokers: Seq[Broker], conditions: Condition*)(implicit ec: ExecutionContext, zk: ZKProxy): Future[Long] = {
    val tasks = getTopicPartitions(topic) map { partition =>
      Future {
        var counter = 0L
        new KafkaMicroConsumer(TopicAndPartition(topic, partition), brokers) use { subs =>
          var offset: Option[Long] = subs.getFirstOffset
          val lastOffset: Option[Long] = subs.getLastOffset
          def eof: Boolean = offset.exists(o => lastOffset.exists(o > _))
          while (!eof) {
            for {
              ofs <- offset
              msg <- subs.fetch(ofs)(DEFAULT_FETCH_SIZE)
            } if (conditions.forall(_.satisfies(msg.message, msg.key))) counter += 1
            offset = offset map (_ + 1)
          }
        }
        counter
      }
    }

    // return the summed count
    Future.sequence(tasks).map(_.sum)
  }

  /**
   * Returns the promise of the option of a message based on the given search criteria
   * @param topic the given topic name
   * @param brokers the given replica brokers
   * @param correlationId the given correlation ID
   * @param conditions the given search criteria
   * @return the promise of a sequence of messages based on the given search criteria
   */
  def findAll(topic: String,
              brokers: Seq[Broker],
              correlationId: Int,
              conditions: Seq[Condition],
              limit: Option[Int],
              counter: IOCounter)(implicit ec: ExecutionContext, zk: ZKProxy): Future[Seq[MessageData]] = {
    val count = new AtomicLong(0L)
    val partitions = getTopicPartitions(topic)
    val tasks = (1 to partitions.size) zip partitions map { case (threadId, partition) =>
      Future {
        var messages: List[MessageData] = Nil
        new KafkaMicroConsumer(TopicAndPartition(topic, partition), brokers) use { subs =>
          var offset: Option[Long] = subs.getFirstOffset
          val lastOffset: Option[Long] = subs.getLastOffset
          def eof: Boolean = offset.exists(o => lastOffset.exists(o > _)) || limit.exists(count.get >= _)
          while (!eof) {
            for {
              ofs <- offset
              msg <- subs.fetch(ofs)(DEFAULT_FETCH_SIZE).headOption
            } {
              counter.updateReadCount(1)
              if (conditions.forall(_.satisfies(msg.message, msg.key))) {
                messages = msg :: messages
                count.incrementAndGet()
              }
            }

            offset = offset map (_ + 1)
          }
          messages.reverse
        }
      }
    }

    // return a promise of the messages
    Future.sequence(tasks) map (_.flatten)
  }

  /**
   * Returns the promise of the option of a message based on the given search criteria
   * @param topic the given topic name
   * @param brokers the given replica brokers
   * @param conditions the given search criteria
   * @return the promise of the option of a message based on the given search criteria
   */
  def findOne(topic: String, brokers: Seq[Broker], forward: Boolean, conditions: Condition*)(implicit ec: ExecutionContext, zk: ZKProxy): Future[Option[(Int, MessageData)]] = {
    val promise = Promise[Option[(Int, MessageData)]]()
    val message = new AtomicReference[Option[(Int, MessageData)]](None)
    val tasks = getTopicPartitions(topic) map { partition =>
      Future {
        new KafkaMicroConsumer(TopicAndPartition(topic, partition), brokers) use { subs =>
          if (forward)
            findOneForward(subs, partition, message, conditions: _*)
          else
            findOneBackward(subs, partition, message, conditions: _*)
        }
      }
    }

    // check for the failure to find a message
    Future.sequence(tasks).onComplete {
      case Success(_) => promise.success(message.get)
      case Failure(e) => promise.failure(e)
    }
    promise.future
  }

  private def findOneForward(subs: KafkaMicroConsumer, partition: Int, message: AtomicReference[Option[(Int, MessageData)]], conditions: Condition*) = {
    var offset: Option[Long] = subs.getFirstOffset
    val lastOffset: Option[Long] = subs.getLastOffset
    def eof: Boolean = offset.exists(o => lastOffset.exists(o > _)) || message.get.isDefined
    while (!eof) {
      for {
        ofs <- offset
        msg <- subs.fetch(ofs)(DEFAULT_FETCH_SIZE)
      } if (conditions.forall(_.satisfies(msg.message, msg.key))) {
        message.compareAndSet(None, Option((partition, msg)))
      }

      offset = offset map (_ + 1)
    }
  }

  private def findOneBackward(subs: KafkaMicroConsumer, partition: Int, message: AtomicReference[Option[(Int, MessageData)]], conditions: Condition*) = {
    var offset: Option[Long] = subs.getLastOffset
    val firstOffset: Option[Long] = subs.getFirstOffset
    def eof: Boolean = offset.exists(o => firstOffset.exists(o < _)) || message.get.isDefined
    while (!eof) {
      for {
        ofs <- offset
        msg <- subs.fetch(ofs)(DEFAULT_FETCH_SIZE)
      } if (conditions.forall(_.satisfies(msg.message, msg.key))) {
        message.compareAndSet(None, Option((partition, msg)))
      }

      offset = offset map (_ - 1)
    }
  }

  /**
   * Returns the promise of the option of a message based on the given search criteria
   * @param tap the given [[TopicAndPartition]]
   * @param brokers the given replica brokers
   * @param conditions the given search criteria
   * @return the promise of the option of a message based on the given search criteria
   */
  def findNext(tap: TopicAndPartition, brokers: Seq[Broker], conditions: Condition*)(implicit ec: ExecutionContext, zk: ZKProxy): Future[Option[MessageData]] = {
    val message = new AtomicReference[Option[MessageData]](None)
    Future {
      new KafkaMicroConsumer(tap, brokers) use { subs =>
        var offset: Option[Long] = subs.getFirstOffset
        val lastOffset: Option[Long] = subs.getLastOffset
        def eof: Boolean = offset.exists(o => lastOffset.exists(o > _)) || message.get.isDefined
        while (!eof) {
          for {
            ofs <- offset
            msg <- subs.fetch(ofs)(DEFAULT_FETCH_SIZE)
          } {
            if (conditions.forall(_.satisfies(msg.message, msg.key))) message.compareAndSet(None, Option(msg))
          }

          offset = offset map (_ + 1)
        }
      }
      message.get
    }
  }

  /**
   * Retrieves the list of defined brokers from Zookeeper
   */
  def getBrokerList(implicit zk: ZKProxy): Seq[BrokerDetails] = {
    val sdf = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss z")
    val basePath = getPrefixedPath("/brokers/ids")
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
  def getConsumerDetails(topicPrefix: Option[String] = None)(implicit zk: ZKProxy): Seq[ConsumerDetails] = {
    val basePath = getPrefixedPath("/consumers")

    // start with the list of consumer IDs
    zk.getChildren(basePath) flatMap { consumerId =>
      // get the list of topics
      val offsetPath = s"$basePath/$consumerId/offsets"
      try {
        val topics = zk.getChildren(offsetPath).distinct filter (contentFilter(topicPrefix, _))

        // get the list of partitions
        topics flatMap { topic =>
          val topicPath = s"$offsetPath/$topic"
          zk.getChildren(topicPath) flatMap { partitionId =>
            val partitionPath = s"$topicPath/$partitionId"
            zk.readString(partitionPath) flatMap { offset =>
              val lastModified = zk.getModificationTime(partitionPath)
              Try(ConsumerDetails(consumerId, topic, partitionId.toInt, offset.toLong, lastModified)).toOption
            }
          }
        }
      } catch {
        case e: Exception =>
          logger.warn("Failed to retrieve consumers", e)
          None
      }
    }
  }

  def getReplicas(topic: String, brokers: Seq[Broker])(implicit zk: ZKProxy): Seq[ReplicaBroker] = {
    val results = for {
      partition <- getTopicPartitions(topic)
      (leader, pmd, replicas) <- getLeaderPartitionMetaDataAndReplicas(TopicAndPartition(topic, partition), brokers)
      inSyncReplicas = pmd.isr map (r => Broker(r.host, r.port, r.id))
    } yield (partition, replicas, inSyncReplicas)

    results flatMap { case (partition, replicas, insSyncReplicas) => replicas map (r =>
      ReplicaBroker(partition, r.host, r.port, r.brokerId, insSyncReplicas.contains(r)))
    }
  }

  /**
   * Retrieves the list of consumers from Zookeeper (Kafka-Storm Partition Manager Version)
   */
  def getStormConsumerList()(implicit zk: ZKProxy): Seq[ConsumerDetailsPM] = {
    zk.getFamily(path = getPrefixedPath("/")).distinct filter (_.matches( """\S+[/]partition_\d+""")) flatMap { path =>
      zk.readString(path) flatMap { jsonString =>
        val lastModified = zk.getModificationTime(path)
        Try {
          val json = parse(jsonString)
          val id = (json \ "topology" \ "id").extract[String]
          val name = (json \ "topology" \ "name").extract[String]
          val topic = (json \ "topic").extract[String]
          val offset = (json \ "offset").extract[Long]
          val partition = (json \ "partition").extract[Int]
          val brokerHost = (json \ "broker" \ "host").extract[String]
          val brokerPort = (json \ "broker" \ "port").extract[Int]
          ConsumerDetailsPM(id, name, topic, partition, offset, lastModified, s"$brokerHost:$brokerPort")
        }.toOption
      }
    }
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
    zk.getChildren(path = getPrefixedPath(s"/brokers/topics/$topic/partitions")) map (_.toInt)
  }

  /**
   * Returns the list of topics for the given brokers
   */
  def getTopicList(brokers: Seq[Broker])(implicit zk: ZKProxy): Seq[TopicDetails] = {
    // get the list of topics
    val topics = zk.getChildren(path = getPrefixedPath("/brokers/topics"))

    // capture the meta data for all topics
    brokers.headOption map { broker =>
      getTopicMetadata(broker, topics) flatMap { tmd =>
        logger.debug(s"Trying to fetch ${tmd.topic}")
        // check for errors
        if (tmd.errorCode != 0) {
          logger.warn(s"Could not read topic ${tmd.topic}, error: ${tmd.errorCode}")
          None
        } else {
          // translate the partition meta data into topic information instances
          tmd.partitionsMetadata flatMap { pmd =>
            // check for errors
            if (pmd.errorCode != 0) {
              logger.warn(s"Could not read partition ${tmd.topic}/${pmd.partitionId}, error: ${pmd.errorCode}")
              None
            } else Some(
              TopicDetails(
                tmd.topic,
                pmd.partitionId,
                pmd.leader map (b => Broker(b.host, b.port, b.id)),
                pmd.replicas map (b => Broker(b.host, b.port, b.id)),
                pmd.isr map (b => Broker(b.host, b.port, b.id)),
                tmd.sizeInBytes)
            )
          }
        }
      }
    } getOrElse Nil
  }

  /**
   * Returns the list of summarized topics for the given brokers
   */
  def getTopicSummaryList(brokers: Seq[Broker])(implicit zk: ZKProxy): Iterable[TopicSummary] = {
    getTopicList(brokers) groupBy (_.topic) map { case (name, partitions) =>
      TopicSummary(name, partitions.map(_.partitionId).max)
    }
  }

  /**
   * Returns the promise of the option of a message based on the given search criteria
   * @param topic the given topic name
   * @param brokers the given replica brokers
   * @param observer the given callback function
   * @return the promise of the option of a message based on the given search criteria
   */
  def observe(topic: String, brokers: Seq[Broker])(observer: MessageData => Unit)(implicit ec: ExecutionContext, zk: ZKProxy): Future[Seq[Unit]] = {
    Future.sequence(getTopicPartitions(topic) map { partition =>
      Future {
        new KafkaMicroConsumer(TopicAndPartition(topic, partition), brokers) use { subs =>
          var offset: Option[Long] = subs.getFirstOffset
          val lastOffset: Option[Long] = subs.getLastOffset
          def eof: Boolean = offset.exists(o => lastOffset.exists(o > _))
          while (!eof) {
            for (ofs <- offset; msg <- subs.fetch(ofs)(DEFAULT_FETCH_SIZE).headOption) observer(msg)
            offset = offset map (_ + 1)
          }
        }
      }
    })
  }

  /**
   * Returns the promise of the option of a message based on the given search criteria
   * @param topic the given topic name
   * @param brokers the given replica brokers
   * @param groupId the given consumer group ID
   * @param commitFrequencyMills the commit frequency in milliseconds
   * @param observer the given callback function
   * @return the promise of the option of a message based on the given search criteria
   */
  def observe(topic: String, brokers: Seq[Broker], groupId: String, commitFrequencyMills: Long)(observer: MessageData => Unit)(implicit ec: ExecutionContext, zk: ZKProxy): Future[Seq[Unit]] = {
    Future.sequence(KafkaMicroConsumer.getTopicPartitions(topic) map { partition =>
      Future {
        var lastUpdate = System.currentTimeMillis()
        new KafkaMicroConsumer(TopicAndPartition(topic, partition), brokers) use { subs =>
          var offset: Option[Long] = subs.getFirstOffset
          val lastOffset: Option[Long] = subs.getLastOffset
          def eof: Boolean = offset.exists(o => lastOffset.exists(o > _))
          while (!eof) {
            // allow the observer to process the messages
            for (ofs <- offset; msg <- subs.fetch(ofs)(fetchSize = 65535).headOption) observer(msg)

            // commit the offset once per second
            if (System.currentTimeMillis() - lastUpdate >= commitFrequencyMills) {
              offset.foreach(subs.commitOffsets(groupId, _, ""))
              lastUpdate = System.currentTimeMillis()
            }
            offset = offset map (_ + 1)
          }

          // commit the final offset for each partition
          lastOffset.foreach(subs.commitOffsets(groupId, _, ""))
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
  private def getLeaderPartitionMetaDataAndReplicas(tap: TopicAndPartition, brokers: Seq[Broker]): Option[(Broker, PartitionMetadata, Seq[Broker])] = {
    for {
      pmd <- brokers.foldLeft[Option[PartitionMetadata]](None)((result, broker) =>
        result ?? getPartitionMetadata(broker, tap).headOption)
      leader <- pmd.leader map (r => Broker(r.host, r.port, r.id))
      replicas = pmd.replicas map (r => Broker(r.host, r.port, r.id))
    } yield (leader, pmd, replicas)
  }

  /**
   * Retrieves the partition meta data for the given broker
   */
  private def getPartitionMetadata(broker: Broker, tap: TopicAndPartition): Seq[PartitionMetadata] = {
    connect(broker, makeClientID("pmd_lookup")) use { consumer =>
      Try {
        consumer
          .send(new TopicMetadataRequest(Seq(tap.topic), correlationId))
          .topicsMetadata
          .flatMap(_.partitionsMetadata.find(_.partitionId == tap.partition))

      } match {
        case Success(pmdSeq) => pmdSeq
        case Failure(e) =>
          throw new VxKafkaTopicException(s"Error communicating with Broker [$broker] to find Leader", tap, e)
      }
    }
  }

  /**
   * Prefixes the given path to support instances where the Zookeeper is either multi-tenant or uses
   * a custom-directory structure.
   * @param path the given Zookeeper/Kafka path
   * @return the prefixed path
   */
  private def getPrefixedPath(path: String) = s"$rootKafkaPath$path".replaceAllLiterally("//", "/")

  /**
   * Retrieves the partition meta data for the given broker
   */
  private def getTopicMetadata(broker: Broker, topics: Seq[String]): Seq[TopicMetadata] = {
    connect(broker, makeClientID("tmd_lookup")) use { consumer =>
      Try {
        consumer
          .send(new TopicMetadataRequest(topics, correlationId))
          .topicsMetadata

      } match {
        case Success(tmdSeq) => tmdSeq
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
  case class ConsumerDetails(consumerId: String, topic: String, partition: Int, offset: Long, lastModified: Option[Long])

  /**
   * Represents the consumer group details for a given topic partition (Kafka Spout / Partition Manager)
   */
  case class ConsumerDetailsPM(topologyId: String, topologyName: String, topic: String, partition: Int, offset: Long, lastModified: Option[Long], broker: String)

  /**
   * Represents a message and offset
   * @param offset the offset of the message within the topic partition
   * @param nextOffset the next available offset
   * @param message the message
   */
  case class MessageData(partition: Int, offset: Long, nextOffset: Long, lastOffset: Long, key: Array[Byte], message: Array[Byte])
    extends BinaryMessage

  case class ReplicaBroker(partition: Int, host: String, port: Int, id: Int, inSync: Boolean)

  /**
   * Represents the details for a Kafka topic
   */
  case class TopicDetails(topic: String, partitionId: Int, leader: Option[Broker], replicas: Seq[Broker], isr: Seq[Broker], sizeInBytes: Int)

  case class TopicSummary(topic: String, partitions: Int)

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
