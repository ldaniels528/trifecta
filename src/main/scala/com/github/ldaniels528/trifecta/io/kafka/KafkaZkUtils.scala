package com.github.ldaniels528.trifecta.io.kafka

import com.github.ldaniels528.trifecta.io.kafka.KafkaMicroConsumer.ConsumerDetailsPM
import com.github.ldaniels528.trifecta.io.kafka.KafkaZkUtils._
import com.github.ldaniels528.trifecta.io.zookeeper.ZKProxy
import net.liftweb.json.parse

import scala.language.postfixOps
import scala.util.Try

/**
  * Kafka-Zookeeper Utilities
  * @author lawrence.daniels@gmail.com
  */
class KafkaZkUtils(var rootKafkaPath: String = "/") {
  private implicit val formats = net.liftweb.json.DefaultFormats

  /**
    * Returns the bootstrap servers as a comma-delimited string
    * @return the bootstrap servers as a comma-delimited string
    */
  def getBootstrapServers(implicit zk: ZKProxy): String = {
    getBrokerList map (b => s"${b.host}:${b.port}") mkString ","
  }

  /**
    * Retrieves the list of defined brokers from Zookeeper
    */
  def getBrokerList(implicit zk: ZKProxy): Seq[BrokerDetails] = {
    val basePath = getPrefixedPath("/brokers/ids")
    for {
      brokerId <- zk.getChildren(basePath)
      brokerPath = s"$basePath/$brokerId"
      json <- zk.readString(brokerPath) if zk.exists(brokerPath)
      details = parse(json).extract[BrokerDetails]
    } yield details
  }

  def getBrokerTopics(implicit zk: ZKProxy): Seq[TopicState] = {
    val basePath = getPrefixedPath("/brokers/topics")
    for {
      topic <- zk.getChildren(basePath)
      partitionsPath = s"$basePath/$topic/partitions"
      partition <- Try(zk.getChildren(partitionsPath)).getOrElse(Nil) if zk.exists(partitionsPath)
      statePath = s"$partitionsPath/$partition/state"
      json <- zk.readString(statePath) if zk.exists(statePath)
      state = parse(json).extract[TopicStateRaw]
    } yield TopicState(
      topic = topic,
      partition = partition.toInt,
      controller_epoch = state.controller_epoch,
      leader_epoch = state.leader_epoch,
      leader = state.leader,
      version = state.version,
      isr = state.isr
    )
  }

  private def check(path: String)(implicit zk: ZKProxy) {
    System.out.println(s"path: '$path' (exists? ${zk.exists(path)})")
  }

  def getBrokerTopicNames(implicit zk: ZKProxy): Seq[String] = {
    zk.getChildren(path = getPrefixedPath("/brokers/topics"))
  }

  /**
    * Returns the list of partitions for the given topic
    */
  def getBrokerTopicPartitions(topic: String)(implicit zk: ZKProxy): Seq[Int] = {
    val basePath = getPrefixedPath(s"/brokers/topics/$topic/partitions")
    if (zk.exists(basePath)) zk.getChildren(basePath) map (_.toInt) else Nil
  }

  def getConsumerDetails(implicit zk: ZKProxy): Seq[ConsumerDetails] = {
    for {
      groupId <- getConsumerGroupIds
      consumerOffset <- Try(getConsumerOffsets(groupId)).getOrElse(Nil)
      threads = Try(getConsumerThreads(groupId)).getOrElse(Nil)
      consumerOwners = Try(getConsumerOwners(groupId)).getOrElse(Nil)

      // lookup the owner object
      consumerOwner = consumerOwners.find(o =>
        o.topic == consumerOffset.topic &&
          o.partition == consumerOffset.partition)

      // lookup the thread object
      thread = threads.find(t => t.topic == consumerOffset.topic)

    } yield ConsumerDetails(
      consumerId = groupId,
      version = thread.map(_.version),
      threadId = consumerOwner.map(_.threadId),
      topic = consumerOffset.topic,
      partition = consumerOffset.partition,
      offset = consumerOffset.offset,
      lastModified = consumerOffset.lastModifiedTime,
      lastModifiedISO = consumerOffset.lastModifiedTime.flatMap(toISODateTime(_).toOption)
    )
  }

  def getConsumerGroup(groupId: String)(implicit zk: ZKProxy): Option[ConsumerGroup] = {
    Option(ConsumerGroup(
      consumerId = groupId,
      offsets = Try(getConsumerOffsets(groupId)).getOrElse(Nil),
      owners = Try(getConsumerOwners(groupId)).getOrElse(Nil),
      threads = Try(getConsumerThreads(groupId)).getOrElse(Nil)
    ))
  }

  def getConsumerGroupIds(implicit zk: ZKProxy): Seq[String] = {
    zk.getChildren(path = getPrefixedPath("/consumers"))
  }

  /**
    * Retrieves consumer owner information from zk:/consumers/<groupId>/owners
    * @param groupId the given consumer group ID
    * @param zk      the implicit [[ZKProxy]]
    * @return a collection of [[ConsumerOwner]]s
    */
  def getConsumerOwners(groupId: String)(implicit zk: ZKProxy): Seq[ConsumerOwner] = {
    val ownersPath = getPrefixedPath(s"/consumers/$groupId/owners")

    // retrieve the owners: /consumers/<groupId>/owners/<topic>/<partition>
    for {
      topic <- zk.getChildren(ownersPath)
      consumerPath = s"$ownersPath/$topic"
      partitionId <- zk.getChildren(consumerPath).map(_.trim)
      consumerOffsetPath = s"$consumerPath/$partitionId" if partitionId.nonEmpty
      consumerThreadId <- zk.readString(consumerOffsetPath) if zk.exists(consumerOffsetPath)
    } yield ConsumerOwner(groupId, topic, consumerThreadId, partitionId.toInt)
  }

  /**
    * Retrieves consumer offset information from zk:/consumers/<groupId>/offsets
    * @param groupId the given consumer group ID
    * @param zk      the implicit [[ZKProxy]]
    * @return a collection of [[ConsumerOffset]]s
    */
  def getConsumerOffsets(groupId: String)(implicit zk: ZKProxy): Seq[ConsumerOffset] = {
    val offsetsPath = getPrefixedPath(s"/consumers/$groupId/offsets")

    // retrieve the owners: /consumers/<groupId>/offsets/<topic>/<partition>
    for {
      topic <- zk.getChildren(offsetsPath)
      consumerTopicPath = s"$offsetsPath/$topic"
      partitionId <- zk.getChildren(consumerTopicPath) if zk.exists(consumerTopicPath)
      consumerPartitionPath = s"$consumerTopicPath/$partitionId"
      consumerOffset <- zk.readString(consumerPartitionPath) if zk.exists(consumerPartitionPath)
      lastModifiedTime = zk.getModificationTime(consumerPartitionPath)
    } yield ConsumerOffset(groupId, topic, partitionId.toInt, consumerOffset.toLong, lastModifiedTime)
  }

  /**
    * Retrieves consumer thread information from zk:/consumers/<groupId>/ids
    * @param groupId the given consumer group ID
    * @param zk      the implicit [[ZKProxy]]
    * @return a collection of [[ConsumerThread]]s
    */
  def getConsumerThreads(groupId: String)(implicit zk: ZKProxy): Seq[ConsumerThread] = {
    val idsPath = getPrefixedPath(s"/consumers/$groupId/ids")

    // retrieve the owners: /consumers/<groupId>/ids/<threadId>
    // {"version":1,"subscription":{"birf_json_qa_pibv":4},"pattern":"static","timestamp":"1483744242777"}
    (for {
      threadId <- zk.getChildren(idsPath)
      threadInfoPath = s"$idsPath/$threadId"
      json <- zk.readString(threadInfoPath).map(_.trim) if zk.exists(threadInfoPath)
      jsObj = parse(json).extract[ConsumerThreadRaw] if json.nonEmpty
      threads = jsObj.subscription map { case (topic, _) =>
        ConsumerThread(
          version = jsObj.version,
          groupId = groupId,
          threadId = threadId,
          topic = topic,
          timestamp = jsObj.timestamp,
          timestampISO = jsObj.timestampISO)
      } toSeq
    } yield threads).flatten
  }

  /**
    * Retrieves the list of consumers from Zookeeper (Kafka-Storm Partition Manager Version)
    */
  def getConsumersForStorm()(implicit zk: ZKProxy): Seq[ConsumerDetailsPM] = {
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
        } toOption
      }
    }
  }

  /**
    * Prefixes the given path to support instances where the Zookeeper is either multi-tenant or uses
    * a custom-directory structure.
    * @param path the given Zookeeper/Kafka path
    * @return the prefixed path
    */
  def getPrefixedPath(path: String): String = s"$rootKafkaPath$path".replaceAllLiterally("//", "/")

}

/**
  * Kafka-Zookeeper Utilities
  * @author lawrence.daniels@gmail.com
  */
object KafkaZkUtils {

  def toISODateTime(ts: String): Try[String] = {
    val sdf = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss z")
    Try(sdf.format(new java.util.Date(ts.toLong)))
  }

  def toISODateTime(ts: Long): Try[String] = {
    val sdf = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss z")
    Try(sdf.format(new java.util.Date(ts)))
  }

  /**
    * Object representation of JSON broker information
    * @example {{{ {"jmx_port":9999,"timestamp":"1405818758964","host":"dev502","version":1,"port":9092} }}}
    */
  case class BrokerDetails(jmx_port: Int, timestamp: String, host: String, version: Int, port: Int) {
    lazy val timestampISO: Option[String] = toISODateTime(timestamp).toOption
  }

  /**
    * Represents the consumer group details for a given topic partition
    */
  case class ConsumerDetails(version: Option[Int], consumerId: String, threadId: Option[String], topic: String, partition: Int, offset: Long, lastModified: Option[Long], lastModifiedISO: Option[String])

  case class ConsumerGroup(consumerId: String, offsets: Seq[ConsumerOffset], owners: Seq[ConsumerOwner], threads: Seq[ConsumerThread])

  case class ConsumerOwner(groupId: String, topic: String, threadId: String, partition: Int)

  case class ConsumerOffset(groupId: String, topic: String, partition: Int, offset: Long, lastModifiedTime: Option[Long])

  /**
    * Object representation of JSON consumer thread
    * @param version      the Kafka protocol version number
    * @param subscription the topic/partition subscription mapping
    * @param pattern      the pattern (e.g. "static")
    * @param timestamp    the last updated time
    * @example {{{ {"version":1,"subscription":{"birf_json_qa_pibv":4},"pattern":"static","timestamp":"1483744242777"} }}}
    */
  case class ConsumerThreadRaw(version: Int, subscription: Map[String, Int], pattern: String, timestamp: String) {
    lazy val timestampISO: Option[String] = toISODateTime(timestamp).toOption
  }

  case class ConsumerThread(version: Int, groupId: String, threadId: String, topic: String, timestamp: String, timestampISO: Option[String])

  case class TopicState(topic: String, partition: Int, controller_epoch: Int, leader_epoch: Int, leader: Int, version: Int, isr: Seq[Int])

  case class TopicStateRaw(controller_epoch: Int, leader_epoch: Int, leader: Int, version: Int, isr: Seq[Int])

  case class TopicWithPartition(topic: String, partitionCount: Int)

}