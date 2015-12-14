package com.ldaniels528.trifecta.modules

import java.io.PrintStream
import java.text.SimpleDateFormat
import java.util.Date

import _root_.kafka.common.TopicAndPartition
import com.ldaniels528.trifecta.TxResultHandler.Ok
import com.ldaniels528.trifecta.command._
import com.ldaniels528.trifecta.io.avro.AvroConversion._
import com.ldaniels528.trifecta.io.avro.{AvroCodec, AvroDecoder, AvroMessageDecoding}
import com.ldaniels528.trifecta.io.kafka.KafkaCliFacade._
import com.ldaniels528.trifecta.io.kafka.KafkaMicroConsumer.{MessageData, contentFilter}
import com.ldaniels528.trifecta.io.kafka._
import com.ldaniels528.trifecta.io.zookeeper.ZKProxy
import com.ldaniels528.trifecta.io.{AsyncIO, InputSource, KeyAndMessage, OutputSource}
import com.ldaniels528.trifecta.messages.logic.Condition
import com.ldaniels528.trifecta.messages.logic.Expressions.{AND, Expression, OR}
import com.ldaniels528.trifecta.messages.{BinaryMessage, MessageDecoder}
import com.ldaniels528.trifecta.modules.ModuleHelper._
import com.ldaniels528.commons.helpers.OptionHelper._
import com.ldaniels528.trifecta.util.ParsingHelper._
import com.ldaniels528.commons.helpers.ResourceHelper._
import com.ldaniels528.commons.helpers.StringHelper._
import com.ldaniels528.commons.helpers.TimeHelper.Implicits._
import com.ldaniels528.trifecta.{TxConfig, TxRuntimeContext}
import net.liftweb.json.JValue
import org.apache.avro.generic.GenericRecord

import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent._
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.{Failure, Success, Try}

/**
 * Apache Kafka Module
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class KafkaModule(config: TxConfig) extends Module {
  private var zkProxy_? : Option[ZKProxy] = None
  private val out: PrintStream = config.out

  // incoming messages cache
  private var incomingMessageCache = Map[TopicAndPartition, Inbound]()
  private var lastInboundCheck: Long = _

  // define the offset for message cursor navigation commands
  private val navigableCursors = mutable.Map[String, KafkaNavigableCursor]()
  private val watchCursors = mutable.Map[TopicAndGroup, KafkaWatchCursor]()
  private var currentTopic: Option[String] = None
  private var currentTopicAndGroup: Option[TopicAndGroup] = None
  private var watching: Boolean = false

  // create the facade
  private val facade = new KafkaCliFacade(config)

  /**
   * Returns the list of brokers from Zookeeper
   * @return the list of [[Broker]]s
   */
  private lazy val brokers: Seq[Broker] = facade.brokers

  def defaultFetchSize: Int = config.getOrElse("defaultFetchSize", "65536").toInt

  def defaultFetchSize_=(sizeInBytes: Int) = config.set("defaultFetchSize", sizeInBytes.toString)

  // the bound commands
  override def getCommands(implicit rt: TxRuntimeContext): Seq[Command] = Seq(
    // connection-related commands
    Command(this, "kconnect", connect, UnixLikeParams(Seq("host" -> false, "port" -> false)), help = "Establishes a connection to Zookeeper"),
    Command(this, "ksandbox", sandBox, UnixLikeParams(), help = "Launches a Kafka Sandbox (local/embedded server instance)"),

    // basic message creation & retrieval commands
    Command(this, "kget", getMessage, UnixLikeParams(Seq("topic" -> false, "partition" -> false, "offset" -> false), Seq("-a" -> "avroCodec", "-f" -> "format", "-o" -> "outputSource", "-p" -> "partition", "-ts" -> "YYYY-MM-DDTHH:MM:SS")), help = "Retrieves the message at the specified offset for a given topic partition"),
    Command(this, "kgetkey", getMessageKey, UnixLikeParams(Seq("topic" -> false, "partition" -> false, "offset" -> false), Seq("-f" -> "format", "-s" -> "fetchSize")), help = "Retrieves the key of the message at the specified offset for a given topic partition"),
    Command(this, "kgetsize", getMessageSize, UnixLikeParams(Seq("topic" -> false, "partition" -> false, "offset" -> false), Seq("-s" -> "fetchSize")), help = "Retrieves the size of the message at the specified offset for a given topic partition"),
    Command(this, "kput", publishMessage, UnixLikeParams(Seq("topic" -> false, "key" -> true, "message" -> true)), help = "Publishes a message to a topic"),

    // consumer group-related commands
    Command(this, "kcommit", commitOffset, UnixLikeParams(Seq("topic" -> false, "partition" -> false, "groupId" -> true, "offset" -> true), Seq("-m" -> "metadata")), help = "Commits the offset for a given topic and group"),
    Command(this, "kconsumers", getConsumers, UnixLikeParams(Nil, Seq("-t" -> "topicPrefix", "-c" -> "consumerPrefix")), help = "Returns a list of the consumers from ZooKeeper"),
    Command(this, "kfetch", fetchOffsets, UnixLikeParams(Seq("topic" -> false, "partition" -> false, "groupId" -> true)), help = "Retrieves the offset for a given topic and group"),
    Command(this, "kreset", resetConsumerGroup, UnixLikeParams(Seq("topic" -> false, "groupId" -> false)), help = "Sets a consumer group ID to zero for all partitions"),
    Command(this, "kwatch", watchConsumerGroup, UnixLikeParams(Seq("topic" -> false, "groupId" -> false), Seq("-a" -> "avroCodec")), help = "Creates a message watch cursor for a given topic"),
    Command(this, "kwatchnext", watchNext, UnixLikeParams(Seq("topic" -> false, "groupId" -> false), Seq("-a" -> "avroCodec")), help = "Returns the next message from the watch cursor"),
    Command(this, "kwatchstop", watchStop, UnixLikeParams(Seq("topic" -> false, "groupId" -> false)), help = "Stops watching a consumer for a given topic"),

    // navigable cursor-related commands
    Command(this, "kcursor", getNavigableCursor, UnixLikeParams(Nil, Seq("-t" -> "topicPrefix")), help = "Displays the message cursor(s)"),
    Command(this, "kfirst", getFirstMessage, UnixLikeParams(Seq("topic" -> false, "partition" -> false), Seq("-a" -> "avroCodec", "-f" -> "format", "-o" -> "outputSource", "-p" -> "partition")), help = "Returns the first message for a given topic"),
    Command(this, "klast", getLastMessage, UnixLikeParams(Seq("topic" -> false, "partition" -> false), Seq("-a" -> "avroCodec", "-f" -> "format", "-o" -> "outputSource", "-p" -> "partition")), help = "Returns the last message for a given topic"),
    Command(this, "knext", getNextMessage, UnixLikeParams(Seq("delta" -> false), flags = Seq("-a" -> "avroCodec", "-f" -> "format", "-o" -> "outputSource")), help = "Attempts to retrieve the next message"),
    Command(this, "kprev", getPreviousMessage, UnixLikeParams(Seq("delta" -> false), flags = Seq("-a" -> "avroCodec", "-f" -> "format", "-o" -> "outputSource")), help = "Attempts to retrieve the message at the previous offset"),
    Command(this, "kswitch", switchCursor, UnixLikeParams(Seq("topic" -> true)), help = "Switches the currently active topic cursor"),

    // query-related commands
    Command(this, "copy", copyMessages, UnixLikeParams(Nil, Seq("-a" -> "avroSchema", "-i" -> "inputSource", "-o" -> "outputSource", "-n" -> "numRecordsToCopy")), help = "Copies messages from an input source to an output source"),
    Command(this, "kcount", countMessages, UnixLikeParams(Seq("field" -> true, "operator" -> true, "value" -> true), Seq("-a" -> "avroCodec", "-t" -> "topic")), help = "Counts the messages matching a given condition"),
    Command(this, "kfind", findMessages, UnixLikeParams(Seq("field" -> true, "operator" -> true, "value" -> true), Seq("-a" -> "avroCodec", "-o" -> "outputSource", "-t" -> "topic")), "Finds messages matching a given condition and exports them to a topic"),
    Command(this, "kfindone", findOneMessage, UnixLikeParams(Seq("field" -> true, "operator" -> true, "value" -> true), Seq("-a" -> "avroCodec", "-b" -> "backwards", "-f" -> "format", "-o" -> "outputSource", "-t" -> "topic")), "Returns the first occurrence of a message matching a given condition"),
    Command(this, "kfindnext", findNextMessage, UnixLikeParams(Seq("field" -> true, "operator" -> true, "value" -> true), Seq("-a" -> "avroCodec", "-f" -> "format", "-o" -> "outputSource", "-p" -> "partition", "-t" -> "topic")), "Returns the first occurrence of a message matching a given condition"),
    Command(this, "kgetminmax", getMessageMinMaxSize, UnixLikeParams(Seq("topic" -> false, "partition" -> false, "startOffset" -> true, "endOffset" -> true), Seq("-s" -> "fetchSize")), help = "Retrieves the smallest and largest message sizes for a range of offsets for a given partition"),

    // topic/message information and statistics commands
    Command(this, "kbrokers", getBrokers, UnixLikeParams(), help = "Returns a list of the brokers from ZooKeeper"),
    Command(this, "kreplicas", getReplicas, UnixLikeParams(Seq("topic" -> true)), help = "Returns a list of the replicas for a topic"),
    Command(this, "kfetchsize", fetchSizeGetOrSet, UnixLikeParams(Seq("fetchSize" -> false)), help = "Retrieves or sets the default fetch size for all Kafka queries"),
    Command(this, "kinbound", inboundMessages, UnixLikeParams(Seq("topicPrefix" -> false), Seq("-w" -> "wait-time")), help = "Retrieves a list of topics with new messages (since last query)"),
    Command(this, "kls", getTopics, UnixLikeParams(Seq("topicPrefix" -> false), Seq("-l" -> "listMode")), help = "Lists all existing topics"),
    Command(this, "kstats", getStatistics, UnixLikeParams(Seq("topic" -> false, "beginPartition" -> false, "endPartition" -> false)), help = "Returns the partition details for a given topic")
  )

  /**
   * Returns a Kafka Topic input source
   * @param url the given input URL (e.g. "topic:shocktrade.quotes.avro")
   * @return the option of a Kafka Topic input source
   */
  override def getInputSource(url: String): Option[KafkaTopicInputSource] = {
    url.extractProperty("topic:") map (new KafkaTopicInputSource(brokers, _))
  }

  /**
   * Returns a Kafka Topic output source
   * @param url the given output URL (e.g. "topic:shocktrade.quotes.avro")
   * @return the option of a Kafka Topic output source
   */
  override def getOutputSource(url: String): Option[KafkaTopicOutputSource] = {
    url.extractProperty("topic:") map (new KafkaTopicOutputSource(brokers, _))
  }

  override def moduleName = "kafka"

  override def moduleLabel = "kafka"

  override def prompt: String = {
    (if (navigableCursor.isEmpty || (watching && watchCursor.isDefined)) promptForWatchCursor
    else promptForNavigableCursor) getOrElse "/"
  }

  private def promptForNavigableCursor: Option[String] = navigableCursor map (c => s"${c.topic}/${c.partition}:${c.offset}")

  private def promptForWatchCursor: Option[String] = watchCursor map (g => s"[w]${g.topic}/${g.partition}:${g.offset}")

  override def shutdown() = zkProxy_?.foreach(_.close())

  override def supportedPrefixes: Seq[String] = Seq("topic")

  /**
   * Returns the cursor for the current topic partition
   * @return the cursor for the current topic partition
   */
  private def navigableCursor: Option[KafkaNavigableCursor] = currentTopic.flatMap(navigableCursors.get)

  /**
   * Returns the cursor for the current topic partition
   * @return the cursor for the current topic partition
   */
  private def watchCursor: Option[KafkaWatchCursor] = currentTopicAndGroup.flatMap(watchCursors.get)

  /**
   * Commits the offset for a given topic and group ID
   * @example kcommit com.shocktrade.alerts 0 devc0 123678
   * @example kcommit devc0 123678
   */
  def commitOffset(params: UnixLikeArgs) {
    // get the arguments (topic, partition, groupId and offset)
    val (topic, partition, groupId, offset) = params.args match {
      case aGroupId :: anOffset :: Nil => navigableCursor map (c => (c.topic, c.partition, aGroupId, parseOffset(anOffset))) getOrElse dieNoCursor
      case aTopic :: aPartition :: aGroupId :: anOffset :: Nil => (aTopic, parsePartition(aPartition), aGroupId, parseOffset(anOffset))
      case _ => dieSyntax(params)
    }

    // commit the offset
    facade.commitOffset(topic, partition, groupId, offset, params("-m"))
  }

  /**
   * Establishes a connection to Zookeeper
   * @example kconnect
   * @example kconnect localhost
   * @example kconnect localhost:2181
   */
  def connect(params: UnixLikeArgs) {
    // determine the requested end-point
    val connectionString = params.args match {
      case Nil => config.zooKeeperConnect
      case zconnectString :: Nil => zconnectString
      case _ => dieSyntax(params)
    }

    // connect to the remote peer
    zkProxy_?.foreach(_.close())
    zkProxy_? = Option(ZKProxy(connectionString))
  }

  /**
   * Copy messages from the specified input source to an output source
   * @return the I/O count
   * @example copy -i topic:greetings -o topic:greetings2
   * @example copy -i topic:shocktrade.keystats.avro -o file:json:/tmp/keystats.json -a file:avro/keyStatistics.avsc
   * @example copy -i topic:shocktrade.keystats.avro -o es:/quotes/keystats/$symbol -a file:avro/keyStatistics.avsc
   * @example copy -i topic:shocktrade.quotes.avro -o file:json:/tmp/quotes.json -a file:avro/quotes.avsc
   * @example copy -i topic:quotes.avro -o es:/quotes/$exchange/$symbol -a file:avro/quotes.avsc
   */
  def copyMessages(params: UnixLikeArgs)(implicit rt: TxRuntimeContext): AsyncIO = {
    // get the input source
    val inputSource = getInputSource(params) getOrElse die("No input source defined")

    // get the output source
    val outputSource = getOutputSource(params) getOrElse die("No output source defined")

    // get an optional decoder
    val decoder = getAvroDecoder(params)(rt.config)

    // copy the messages from the input source to the output source
    copyOperation(inputSource, outputSource, decoder)
  }

  /**
   * Copies messages from the input source to the output source
   * @return an asynchronous I/O result
   */
  private def copyOperation(reader: InputSource, writer: OutputSource, decoder: Option[AvroDecoder]) = {
    AsyncIO { counter =>
      var found: Boolean = true

      reader use { in =>
        writer use { out =>
          while (found) {
            // read the record
            val data = reader.read
            found = data.isDefined
            if (found) counter.updateReadCount(1)

            // write the record
            data.foreach { rec =>
              writer.write(rec, decoder)
              counter.updateWriteCount(1)
            }
          }
        }
      }
    }
  }

  /**
   * Launches a Kafka Sandbox (local server instance)
   * @example ksandbox
   */
  def sandBox(params: UnixLikeArgs): Unit = {
    val instance = KafkaSandbox()
    connect(UnixLikeArgs(Some("ksandbox"), List(instance.getConnectString)))
  }

  /**
   * Counts the messages matching a given condition [references cursor]
   * @example kcount frequency >= 1200
   */
  def countMessages(params: UnixLikeArgs): Future[Long] = {
    // was a topic and/or Avro decoder specified?
    val topic_? = params("-t")
    val avro_? = getAvroDecoder(params)(config)

    // get the input topic and decoder from the cursor
    val (topic, decoder) = {
      if (topic_?.isDefined) (topic_?.get, avro_?)
      else navigableCursor map (c => (c.topic, if (avro_?.isDefined) avro_? else c.decoder)) getOrElse dieNoCursor
    }

    // get the criteria
    val conditions = Seq(parseCondition(params, decoder))

    // perform the count
    facade.countMessages(topic, conditions, decoder)
  }

  /**
   * Returns the offsets for a given topic and group ID
   * @example kfetch com.shocktrade.alerts 0 dev
   * @example kfetch dev
   */
  def fetchOffsets(params: UnixLikeArgs): Option[Long] = {
    // get the arguments (topic, partition, groupId)
    val (topic, partition, groupId) = params.args match {
      case aGroupId :: Nil => navigableCursor map (c => (c.topic, c.partition, aGroupId)) getOrElse dieNoCursor
      case aTopic :: aPartition :: aGroupId :: Nil => (aTopic, parsePartition(aPartition), aGroupId)
      case _ => dieSyntax(params)
    }

    // perform the action
    facade.fetchOffsets(topic, partition, groupId)
  }

  /**
   * Retrieves or sets the default fetch size for all Kafka queries
   * @example kfetchsize
   * @example kfetchsize 65536
   */
  def fetchSizeGetOrSet(params: UnixLikeArgs) = {
    params.args.headOption match {
      case Some(fetchSize) => defaultFetchSize = parseInt("fetchSize", fetchSize); ()
      case None => defaultFetchSize
    }
  }

  /**
   * Returns the first message that corresponds to the given criteria
   * @example kfindone volume > 1000000
   * @example kfindone volume > 1000000 -a file:avro/quotes.avsc
   * @example kfindone volume > 1000000 -t shocktrade.quotes.avro -a file:avro/quotes.avsc
   * @example kfindone lastTrade < 1 and volume > 1000000 -a file:avro/quotes.avsc -b true
   */
  def findOneMessage(params: UnixLikeArgs)(implicit rt: TxRuntimeContext) = {
    // was a topic and/or Avro decoder specified?
    val topic_? = params("-t")
    val avro_? = getAvroDecoder(params)(config)
    val forward = !params("-b").exists(parseBoolean("backward", _))

    // get the topic and partition from the cursor
    val (topic, decoder_?) = {
      if (topic_?.isDefined) (topic_?.get, avro_?)
      else navigableCursor map (c => (c.topic, if (avro_?.isDefined) avro_? else c.decoder)) getOrElse dieNoCursor
    }

    // perform the search
    KafkaMicroConsumer.findOne(topic, brokers, forward, parseCondition(params, decoder_?)) map {
      _ map { case (partition, md) =>
        getMessage(topic, partition, md.offset, params)
      }
    }
  }

  /**
   * Returns the first next message that corresponds to the given criteria starting from the current position
   * within the current partition.
   * @example kfindnext volume > 1000000
   * @example kfindnext volume > 1000000 -a file:avro/quotes.avsc
   * @example kfindnext volume > 1000000 -t shocktrade.quotes.avro -p 5 -a file:avro/quotes.avsc
   */
  def findNextMessage(params: UnixLikeArgs)(implicit rt: TxRuntimeContext) = {
    // was a topic, partition and/or Avro decoder specified?
    val topic_? = params("-t")
    val partition_? = params("-p") map parsePartition
    val avro_? = getAvroDecoder(params)(config)

    // get the topic and partition from the cursor
    val (topic, partition, decoder_?) = {
      navigableCursor.map(c => (topic_? getOrElse c.topic, partition_? getOrElse c.partition, avro_?))
        .getOrElse {
        topic_?.map(t => (t, partition_? getOrElse 0, avro_?)) getOrElse dieNoCursor
      }
    }

    // get the criteria
    val condition = parseCondition(params, decoder_?)

    // perform the search
    KafkaMicroConsumer.findNext(TopicAndPartition(topic, partition), brokers, condition) map {
      _ map (md => getMessage(topic, partition, md.offset, params))
    }
  }

  /**
   * Finds messages that corresponds to the given criteria and exports them to a topic
   * @example kfind frequency > 5000 -o topic:highFrequency.quotes
   * @example kfind -t shocktrade.quotes.avro -a file:avro/quotes.avsc volume > 1000000 -o topic:hft.shocktrade.quotes.avro
   */
  def findMessages(params: UnixLikeArgs)(implicit rt: TxRuntimeContext): AsyncIO = {
    // was a topic and/or Avro decoder specified?
    val topic_? = params("-t")
    val avro_? = getAvroDecoder(params)(config)

    // get the input topic and decoder from the cursor
    val (topic, decoder_?) = {
      if (topic_?.isDefined) (topic_?.get, avro_?)
      else navigableCursor map (c => (c.topic, if (avro_?.isDefined) avro_? else c.decoder)) getOrElse dieNoCursor
    }

    // get the criteria
    val conditions = Seq(parseCondition(params, decoder_?))

    // get the output handler
    val outputHandler = params("-o") flatMap rt.getOutputHandler getOrElse die("Output source URL expected")

    // find the messages
    facade.findMessages(topic, decoder_?, conditions, outputHandler)
  }

  /**
   * Retrieves the list of Kafka brokers
   */
  def getBrokers(params: UnixLikeArgs) = KafkaMicroConsumer.getBrokerList

  /**
   * Retrieves the list of Kafka replicas for a given topic
   */
  def getReplicas(params: UnixLikeArgs) = {
    params.args match {
      case topic :: Nil => KafkaMicroConsumer.getReplicas(topic, brokers) sortBy (_.partition)
      case _ => dieSyntax(params)
    }
  }

  /**
   * Retrieves the navigable message cursor for the current topic
   * @example kcursor
   * @example kcursor shocktrade.keystats.avro
   */
  def getNavigableCursor(params: UnixLikeArgs): Seq[KafkaNavigableCursor] = {
    // get the topic & consumer prefixes
    val topicPrefix = params("-t")

    // filter the cursors by topic prefix
    navigableCursors.values.filter(c => contentFilter(topicPrefix, c.topic)).toSeq
  }

  /**
   * Sets the navigable message cursor
   */
  private def setNavigableCursor(topic: String, partition: Int, messageData: Option[MessageData], decoder: Option[MessageDecoder[_]]) {
    messageData map (m => KafkaNavigableCursor(topic, partition, m.offset, m.nextOffset, decoder)) foreach (navigableCursors(topic) = _)
    currentTopic = Option(topic)
    watching = false
  }

  /**
   * Switches between topic cursors
   * @example kswitch shocktrade.keystats.avro
   */
  def switchCursor(params: UnixLikeArgs) {
    for {
      topic <- params.args.headOption
      cursor <- navigableCursors.get(topic)
    } {
      currentTopic = Option(topic)
      watching = false
    }
  }

  /**
   * Retrieves the fetch size (-s) from the given parameters
   * @param params the given Unix-style parameters
   * @return the fetch size
   */
  private def getFetchSize(params: UnixLikeArgs): Int = {
    params("-s") map (parseInt("fetchSize", _)) getOrElse defaultFetchSize
  }

  /**
   * Returns the first message for a given topic
   * @example kfirst
   * @example kfirst -p 5
   * @example kfirst com.shocktrade.quotes.csv 0
   */
  def getFirstMessage(params: UnixLikeArgs)(implicit rt: TxRuntimeContext) = {
    // get the arguments
    val (topic, partition0) = extractTopicAndPartition(params.args)

    // check for a partition override flag
    val partition = params("-p") map parsePartition getOrElse partition0

    // return the first message for the topic partition
    facade.getFirstOffset(topic, partition) map (getMessage(topic, partition, _, params))
  }

  /**
   * Returns the last offset for a given topic
   * @example klast
   * @example klast -p 5
   * @example klast com.shocktrade.alerts 0
   */
  def getLastMessage(params: UnixLikeArgs)(implicit rt: TxRuntimeContext) = {
    // get the arguments
    val (topic, partition0) = extractTopicAndPartition(params.args)

    // check for a partition override flag
    val partition: Int = params("-p") map parsePartition getOrElse partition0

    // return the last message for the topic partition
    facade.getLastOffset(topic, partition) map (getMessage(topic, partition, _, params))
  }

  /**
   * Returns the message for a given topic partition and offset
   * @example kget 3456
   * @example kget com.shocktrade.alerts 0 3456
   * @example kget -o es:/quotes/quote/AAPL
   */
  def getMessage(params: UnixLikeArgs)(implicit rt: TxRuntimeContext): Either[Option[MessageData], Either[Option[GenericRecord], Option[JValue]]] = {
    // get the arguments
    val (topic, partition0, offset) = extractTopicPartitionAndOffset(params.args)

    // check for a partition override flag
    val partition: Int = params("-p") map parsePartition getOrElse partition0

    // generate and return the message
    getMessage(topic, partition, offset, params)
  }

  /**
   * Retrieves either a binary or decoded message
   * @param topic the given topic
   * @param partition the given partition
   * @param offset the given offset
   * @param params the given Unix-style argument
   * @return either a binary or decoded message
   */
  def getMessage(topic: String, partition: Int, offset: Long, params: UnixLikeArgs)(implicit rt: TxRuntimeContext): Either[Option[MessageData], Either[Option[GenericRecord], Option[JValue]]] = {
    // requesting a message from an instance in time?
    val instant: Option[Long] = params("-ts") map {
      case s if s.matches("\\d+") => s.toLong
      case s if s.matches("\\d{4}[-]\\d{2}-\\d{2}[T]\\d{2}[:]\\d{2}[:]\\d{2}") => new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss").parse(s).getTime
      case s => throw die(s"Illegal timestamp format '$s' - expected either EPOC (Long) or yyyy-MM-dd'T'HH:mm:ss format")
    }

    // retrieve the message
    val messageData = new KafkaMicroConsumer(TopicAndPartition(topic, partition), brokers) use { consumer =>
      val myOffset: Long = instant flatMap (t => consumer.getOffsetsBefore(t).headOption) getOrElse offset
      consumer.fetch(myOffset)(getFetchSize(params)).headOption
    }

    // determine which decoder to use; either the user specified decoder, cursor's decoder or none
    val decoder: Option[MessageDecoder[_]] = resolveDecoder(topic, params)

    // if a decoder was found, use it to decode the message
    val decodedMessage = decoder.flatMap(decodeMessage(messageData, _))

    // write the message to an output source handler
    messageData.foreach { md =>
      val outputSource = getOutputSource(params)
      outputSource.foreach(_.write(KeyAndMessage(md.key, md.message), decoder))
    }

    // was a format parameter specified?
    val jsonMessage = decodeMessageAsJson(decodedMessage, params)

    // capture the message's offset and decoder
    setNavigableCursor(topic, partition, messageData, decoder)

    // return either a binary message or a decoded message
    if (jsonMessage.isDefined) Right(Right(jsonMessage))
    else if (decodedMessage.isDefined) Right(Left(decodedMessage))
    else Left(messageData)
  }

  /**
   * Resolves the optional decoder URL (e.g. "-a" (Avro)) for the given topic. Note, if the url is "default"
   * then the default decoder (configured in $HOME/.trifecta/decoders) will be used
   * @param topic the given Kafka topic (e.g. "shocktrade.quotes.avro")
   * @param params the given [[UnixLikeArgs]]
   * @param rt the implicit [[TxRuntimeContext]]
   * @return an option of the [[MessageDecoder]]
   */
  private def resolveDecoder(topic: String, params: UnixLikeArgs)(implicit rt: TxRuntimeContext): Option[MessageDecoder[_]] = {
    params("-a") match {
      case Some(url) if url == "default" => rt.resolveDecoder(topic)
      case Some(url) => Option(AvroCodec.resolve(url)) // TODO no error should be thrown here!
      case None => navigableCursors.get(topic) flatMap (_.decoder)
    }
  }

  /**
   * Decodes the given message
   * @param messageData the given option of a message
   * @param aDecoder the given message decoder
   * @return the decoded message
   */
  private def decodeMessage(messageData: Option[BinaryMessage], aDecoder: MessageDecoder[_]): Option[GenericRecord] = {
    // only Avro decoders are supported
    val decoder: AvroMessageDecoding = aDecoder match {
      case avDecoder: AvroMessageDecoding => avDecoder
      case _ => throw new IllegalStateException("Only Avro decoding is supported")
    }

    // decode the message
    for {
      md <- messageData
      rec = decoder.decode(md.message) match {
        case Success(record) => record
        case Failure(e) =>
          throw new IllegalStateException(e.getMessage, e)
      }
    } yield rec
  }

  private def decodeMessageAsJson(decodedMessage: Option[GenericRecord], params: UnixLikeArgs) = {
    import net.liftweb.json.parse
    for {
      format <- params("-f")
      record <- decodedMessage
      jsonMessage <- format match {
        case "json" => Option(parse(record.toString))
        case "avro_json" => Option(parse(transcodeRecordToAvroJson(record, config.encoding)))
        case _ => die( s"""Invalid format type "$format"""")
      }
    } yield jsonMessage
  }

  /**
   * Returns the message key for a given topic partition and offset
   * @example kget com.shocktrade.alerts 0 3456
   * @example kget 3456
   */
  def getMessageKey(params: UnixLikeArgs): Option[Any] = {
    // get the arguments
    val (topic, partition, offset) = extractTopicPartitionAndOffset(params.args)

    // retrieve (or guess) the value's format
    val valueType = params("-f") getOrElse "bytes"

    // retrieve the key
    facade.getMessageKey(topic, partition, offset, getFetchSize(params)) map (decodeValue(_, valueType))
  }

  /**
   * Returns the size of the message for a given topic partition and offset
   * @example kgetsize com.shocktrade.alerts 0 5567
   * @example kgetsize 5567
   */
  def getMessageSize(params: UnixLikeArgs): Option[Int] = {
    // get the arguments (topic, partition, groupId and offset)
    val (topic, partition, offset) = extractTopicPartitionAndOffset(params.args)

    // perform the action
    facade.getMessageSize(topic, partition, offset, getFetchSize(params))
  }

  /**
   * Returns the minimum and maximum message size for a given topic partition and offset range
   * @example kgetmaxsize com.shocktrade.alerts 0 2100 5567
   * @example kgetmaxsize 2100 5567
   */
  def getMessageMinMaxSize(params: UnixLikeArgs): Seq[MessageMaxMin] = {
    // get the arguments (topic, partition, startOffset and endOffset)
    val (topic, partition, startOffset, endOffset) = params.args match {
      case offset0 :: offset1 :: Nil => navigableCursor map (c => (c.topic, c.partition, parseOffset(offset0), parseOffset(offset1))) getOrElse dieNoCursor
      case aTopic :: aPartition :: aStartOffset :: anEndOffset :: Nil => (aTopic, parsePartition(aPartition), parseOffset(aStartOffset), parseOffset(anEndOffset))
      case _ => dieSyntax(params)
    }

    // perform the action
    facade.getMessageMinMaxSize(topic, partition, startOffset, endOffset, getFetchSize(params))
  }

  /**
   * Optionally returns the next message
   * @example knext
   * @example knext +10
   */
  def getNextMessage(params: UnixLikeArgs)(implicit rt: TxRuntimeContext) = {
    navigableCursor map { case KafkaNavigableCursor(topic, partition, offset, nextOffset, decoder) =>
      val delta = params.args.headOption map (parseDelta("position delta", _))
      val theOffset = delta map (nextOffset + _) getOrElse nextOffset
      val lastOffset = facade.getLastOffset(topic, partition)
      if (lastOffset.exists(theOffset > _)) {
        for {
          (min, max) <- facade.getTopicPartitionRange(topic)
          overflowPartition = (partition + 1) % (max + 1)
          overflowOffset <- facade.getFirstOffset(topic, overflowPartition)
        } yield getMessage(topic, overflowPartition, overflowOffset, params)
      }
      else getMessage(topic, partition, theOffset, params)
    }
  }

  /**
   * Optionally returns the previous message
   * @example kprev
   * @example kprev +10
   */
  def getPreviousMessage(params: UnixLikeArgs)(implicit rt: TxRuntimeContext) = {
    navigableCursor map { case KafkaNavigableCursor(topic, partition, offset, nextOffset, decoder) =>
      val delta = params.args.headOption map (parseDelta("position delta", _))
      val theOffset = Math.max(0, delta map (offset - _) getOrElse (offset - 1))
      val firstOffset = facade.getFirstOffset(topic, partition)
      if (firstOffset.exists(theOffset < _)) {
        for {
          (min, max) <- facade.getTopicPartitionRange(topic)
          overflowPartition = if (partition <= min) max else (partition - 1) % (max + 1)
          overflowOffset <- facade.getLastOffset(topic, overflowPartition)
        } yield getMessage(topic, overflowPartition, overflowOffset, params)
      }
      else getMessage(topic, partition, theOffset, params)
    }
  }

  /**
   * Returns the number of available messages for a given topic
   * @example kstats com.shocktrade.alerts 0 4
   * @example kstats com.shocktrade.alerts
   * @example kstats
   */
  def getStatistics(params: UnixLikeArgs): Iterable[TopicOffsets] = {
    // interpret based on the input arguments
    val results = params.args match {
      case Nil =>
        val topic = navigableCursor map (_.topic) getOrElse dieNoCursor
        val partitions = KafkaMicroConsumer.getTopicList(brokers).filter(_.topic == topic).map(_.partitionId)
        if (partitions.nonEmpty) Option((topic, partitions.min, partitions.max)) else None

      case topic :: Nil =>
        val partitions = KafkaMicroConsumer.getTopicList(brokers).filter(_.topic == topic).map(_.partitionId)
        if (partitions.nonEmpty) Option((topic, partitions.min, partitions.max)) else None

      case topic :: aPartition :: Nil =>
        Option((topic, parsePartition(aPartition), parsePartition(aPartition)))

      case topic :: partitionA :: partitionB :: Nil =>
        Option((topic, parsePartition(partitionA), parsePartition(partitionB)))

      case _ =>
        dieSyntax(params)
    }

    results match {
      case Some((topic, partition0, partition1)) =>
        if (navigableCursor.isEmpty) {
          facade.getFirstOffset(topic, partition0) ?? facade.getLastOffset(topic, partition0) map (offset =>
            KafkaNavigableCursor(topic, partition0, offset, offset + 1, None)) foreach (navigableCursors(topic) = _)
          currentTopic = Option(topic)
        }
        facade.getStatisticsData(topic, partition0, partition1)
      case _ => Nil
    }
  }

  /**
   * Returns a list of topics
   * @example kls com.shocktrade.alerts
   * @example kls
   */
  def getTopics(params: UnixLikeArgs): Either[Seq[TopicItem], Seq[TopicItemCompact]] = {
    // get the prefix and compact/detailed list indicator
    val prefix = params("-l") ?? params.args.headOption
    val detailed = params.contains("-l")

    // get the raw topic data
    facade.getTopics(prefix, detailed)
  }

  /**
   * Retrieves a list of all topics with new messages (since last query)
   * @example kinbound com.shocktrade.quotes
   */
  def inboundMessages(params: UnixLikeArgs): Iterable[Inbound] = {
    val prefix = params.args.headOption

    // get the optional wait time parameter
    val waitTime = params("-w") map (parseInt("wait time in seconds", _))

    // is this the initial call to this command?
    if (waitTime.isDefined || incomingMessageCache.isEmpty || (System.currentTimeMillis() - lastInboundCheck) >= 1.hour) {
      out.println("Sampling data; this may take a few seconds...")

      // generate some data to fill the cache
      inboundMessageStatistics()

      // wait for the specified time in second
      Thread.sleep((waitTime getOrElse 3).seconds)
    }

    // capture the current time
    lastInboundCheck = System.currentTimeMillis()

    // get the inbound topic data
    inboundMessageStatistics(prefix)
  }

  /**
   * Generates an iteration of inbound message statistics
   * @param topicPrefix the given topic prefix (e.g. "myTopic123")
   * @return an iteration of inbound message statistics
   */
  private def inboundMessageStatistics(topicPrefix: Option[String] = None): Iterable[Inbound] = {
    // start by retrieving a list of all topics
    val topics = KafkaMicroConsumer.getTopicList(brokers)
      .filter(t => t.topic == topicPrefix.getOrElse(t.topic))
      .groupBy(_.topic)

    // generate the inbound data
    val inboundData = (topics flatMap { case (topic, details) =>
      // get the range of partitions for each topic
      val partitions = details.map(_.partitionId)
      val (beginPartition, endPartition) = (partitions.min, partitions.max)

      // retrieve the statistics for each topic
      facade.getStatisticsData(topic, beginPartition, endPartition) map { o =>
        val prevInbound = incomingMessageCache.get(TopicAndPartition(o.topic, o.partition))
        val lastCheckTime = prevInbound.map(_.lastCheckTime.getTime) getOrElse System.currentTimeMillis()
        val currentTime = System.currentTimeMillis()
        val elapsedTime = 1 + (currentTime - lastCheckTime) / 1000L
        val change = prevInbound map (o.endOffset - _.endOffset) getOrElse 0L
        val rate = BigDecimal(change.toDouble / elapsedTime).setScale(1, BigDecimal.RoundingMode.UP).toDouble
        Inbound(o.topic, o.partition, o.startOffset, o.endOffset, change, rate, new Date(currentTime))
      }
    }).toSeq

    // cache the unfiltered inbound data
    incomingMessageCache = incomingMessageCache ++ Map(inboundData map (i => TopicAndPartition(i.topic, i.partition) -> i): _*)

    // filter out the non-changed records
    inboundData.filterNot(_.change == 0) sortBy (-_.change)
  }

  /**
   * Publishes the given message to a given topic
   * @example kput greetings a0.00.11.22.33.44.ef.11 "Hello World"
   * @example kput a0.00.11.22.33.44.ef.11 "Hello World" (references cursor)
   */
  def publishMessage(params: UnixLikeArgs): Unit = {
    import com.ldaniels528.trifecta.command.parser.CommandParser._

    // get the topic, key and message
    val (topic, key, message) = params.args match {
      case aKey :: aMessage :: Nil => navigableCursor map (c => (c.topic, aKey, aMessage)) getOrElse dieNoCursor
      case aTopic :: aKey :: aMessage :: Nil => (aTopic, aKey, aMessage)
      case _ => dieSyntax(params)
    }

    // convert the key and message to binary
    val keyBytes = if (isDottedHex(key)) parseDottedHex(key) else key.getBytes(config.encoding)
    val msgBytes = if (isDottedHex(message)) parseDottedHex(message) else message.getBytes(config.encoding)

    // publish the message
    facade.publishMessage(topic, keyBytes, msgBytes)
  }

  /**
   * Retrieves the list of Kafka consumers
   * @example kconsumers
   * @example kconsumers -c ld_group
   * @example kconsumers -t shocktrade.keystats.avro
   */
  def getConsumers(params: UnixLikeArgs): Future[List[ConsumerDelta]] = {
    // get the topic & consumer prefixes
    val consumerPrefix = params("-c")
    val topicPrefix = params("-t")

    // get the Kafka consumer groups
    facade.getConsumers(consumerPrefix, topicPrefix, config.consumersPartitionManager)
  }

  /**
   * Sets the offset of a consumer group ID to zero for all partitions
   * @example kreset
   * @example kreset ld_group
   * @example kreset com.shocktrade.quotes.csv ld_group
   */
  def resetConsumerGroup(params: UnixLikeArgs): Unit = {
    val TopicAndGroup(topic, groupId) = getTopicAndGroup(params)
    facade.resetConsumerGroup(topic, groupId)
  }

  /**
   * Watches a consumer group allowing the user to navigate one-direction (forward)
   * through new messages.
   * @example kwatch ld_group
   * @example kwatch com.shocktrade.quotes.avro ld_group
   * @example kwatch com.shocktrade.quotes.avro ld_group -a file:avro/quotes.avsc
   */
  def watchConsumerGroup(params: UnixLikeArgs): Either[Iterable[WatchCursorItem], Option[Future[Any]]] = {
    if (params.args.isEmpty) Left {
      watchCursors map { case (tag, KafkaWatchCursor(topic, groupId, partition, offset, _, _, decoder)) =>
        WatchCursorItem(groupId, topic, partition, offset, decoder)
      }
    }
    else Right {
      val TopicAndGroup(topic, groupId) = getTopicAndGroup(params)

      for {
        (min, max) <- facade.getTopicPartitionRange(topic)
        consumer = KafkaMacroConsumer(zk.connectionString, groupId, Nil: _*)
        iterator = consumer.iterate(topic, (max - min) + 1)
        decoder = params("-a") map AvroCodec.resolve
      } yield {
        val cursor = KafkaWatchCursor(topic, groupId, partition = 0, offset = 0L, consumer, iterator, decoder)
        updateWatchCursor(cursor, cursor.partition, cursor.offset, autoClose = false)
        watchNext(params)
      }
    }
  }

  /**
   * Reads the next message from the watch cursor
   * @example kwatchnext
   * @example kwatchnext ld_group
   * @example kwatchnext com.shocktrade.quotes.avro ld_group
   * @example kwatchnext com.shocktrade.quotes.avro ld_group -a file:avro/quotes.avsc
   */
  def watchNext(params: UnixLikeArgs): Future[Option[Object]] = {
    // get the arguments
    val topicAndGroup = getTopicAndGroup(params)

    // was a decoder defined?
    val decoder_? = params("-a") map AvroCodec.resolve

    Future {
      watchCursors.get(topicAndGroup) flatMap { cursor =>
        val iterator = cursor.iterator
        if (!iterator.hasNext) None
        else {
          val binaryMessage = Option(iterator.next())
          updateWatchCursor(cursor, binaryMessage.map(_.partition).getOrElse(0), binaryMessage.map(_.offset).getOrElse(0L))

          // if a decoder is defined, use it to decode the message
          val decodedMessage = (if (decoder_?.isDefined) decoder_? else cursor.decoder).flatMap(decodeMessage(binaryMessage, _))

          // return either the decoded message or the binary message
          if (decodedMessage.isDefined) decodedMessage else binaryMessage
        }
      }
    }
  }

  /**
   * Stops watching a consumer for a given topic
   * @example kwatchstop
   * @example kwatchstop ld_group
   * @example kwatchstop com.shocktrade.quotes.avro ld_group
   */
  def watchStop(params: UnixLikeArgs): Try[Option[Ok]] = {
    // get the arguments
    val key = getTopicAndGroup(params)

    // if there's already a registered topic & group, close it
    Try(watchCursors.remove(key) map (_.consumer.close()) map (t => Ok()))
  }

  private def getAvroDecoder(params: UnixLikeArgs)(implicit config: TxConfig): Option[AvroDecoder] = {
    params("-a") map AvroCodec.resolve
  }

  private def getTopicAndGroup(params: UnixLikeArgs): TopicAndGroup = {
    params.args match {
      case Nil => watchCursor map (c => TopicAndGroup(c.topic, c.groupId)) getOrElse dieNoCursor
      case groupId :: Nil if watchCursor.isDefined => watchCursor map (c => TopicAndGroup(c.topic, groupId)) getOrElse dieNoCursor
      case groupId :: Nil => navigableCursor map (c => TopicAndGroup(c.topic, groupId)) getOrElse dieNoCursor
      case topic :: groupId :: Nil => TopicAndGroup(topic, groupId)
      case _ => dieSyntax(params)
    }
  }

  private def updateWatchCursor(c: KafkaWatchCursor, partition: Int, offset: Long, autoClose: Boolean = true) {
    val key = TopicAndGroup(c.topic, c.groupId)

    // if there's already a registered topic & group, close it
    if (autoClose) Try(watchCursors.remove(key) foreach (_.consumer.close()))

    // set the current topic & group and create a new cursor
    currentTopicAndGroup = Option(key)
    watchCursors(key) = KafkaWatchCursor(c.topic, c.groupId, partition, offset, c.consumer, c.iterator, c.decoder)

    // update the navigable cursor for the given topic
    navigableCursors(c.topic) = KafkaNavigableCursor(c.topic, partition, offset, offset + 1, c.decoder)
    currentTopic = Option(c.topic)
    watching = true
  }

  /**
   * Retrieves the topic and partition from the given arguments
   * @param args the given arguments
   * @return a tuple containing the topic and partition
   */
  private def extractTopicAndPartition(args: List[String]): (String, Int) = {
    args match {
      case Nil => navigableCursor map (c => (c.topic, c.partition)) getOrElse dieNoCursor
      case aTopic :: Nil => (aTopic, 0)
      case aTopic :: aPartition :: Nil => (aTopic, parsePartition(aPartition))
      case _ => die("Invalid arguments")
    }
  }

  /**
   * Retrieves the topic, partition and offset from the given arguments
   * @param args the given arguments
   * @return a tuple containing the topic, partition and offset
   */
  private def extractTopicPartitionAndOffset(args: List[String]): (String, Int, Long) = {
    args match {
      case Nil => navigableCursor map (c => (c.topic, c.partition, c.offset)) getOrElse dieNoCursor
      case anOffset :: Nil => navigableCursor map (c => (c.topic, c.partition, parseOffset(anOffset))) getOrElse dieNoCursor
      case aTopic :: aPartition :: anOffset :: Nil => (aTopic, parsePartition(aPartition), parseOffset(anOffset))
      case _ => die("Invalid arguments")
    }
  }

  /**
   * Parses a condition statement
   * @param params the given [[UnixLikeArgs]]
   * @param decoder the optional [[MessageDecoder]]
   * @example lastTrade < 1 and volume > 1000000
   * @return a collection of [[Condition]] objects
   */
  private def parseCondition(params: UnixLikeArgs, decoder: Option[MessageDecoder[_]]): Condition = {
    import com.ldaniels528.trifecta.messages.logic.ConditionCompiler._
    import com.ldaniels528.trifecta.messages.query.parser.KafkaQueryParser.deQuote

    val it = params.args.iterator
    var criteria: Option[Expression] = None
    while (it.hasNext) {
      val args = it.take(criteria.size + 3).toList
      criteria = args match {
        case List("and", field, operator, value) => criteria.map(AND(_, compile(field, operator, deQuote(value))))
        case List("or", field, operator, value) => criteria.map(OR(_, compile(field, operator, deQuote(value))))
        case List(field, operator, value) => Option(compile(field, operator, deQuote(value)))
        case _ => dieSyntax(params)
      }
    }
    criteria.map(compile(_, decoder)).getOrElse(dieSyntax(params))
  }

  /**
   * Returns the connected Zookeeper Proxy
   * @return the connected Zookeeper Proxy
   */
  private implicit def zk: ZKProxy = {
    zkProxy_? match {
      case Some(zk) => zk
      case None =>
        val zk = ZKProxy(config.zooKeeperConnect)
        zkProxy_? = Option(zk)
        zk
    }
  }

}
