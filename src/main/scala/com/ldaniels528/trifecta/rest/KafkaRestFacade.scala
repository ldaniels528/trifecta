package com.ldaniels528.trifecta.rest

import java.io.{File, FileOutputStream}
import java.util.UUID
import java.util.concurrent.Executors

import com.ldaniels528.trifecta.TxConfig.TxDecoder
import com.ldaniels528.trifecta.command.parser.CommandParser
import com.ldaniels528.trifecta.io.ByteBufferUtils
import com.ldaniels528.trifecta.io.avro.AvroConversion
import com.ldaniels528.trifecta.io.json.{JsonDecoder, JsonHelper}
import com.ldaniels528.trifecta.io.kafka.KafkaMicroConsumer.{DEFAULT_FETCH_SIZE, LeaderAndReplicas, MessageData}
import com.ldaniels528.trifecta.io.kafka.{Broker, KafkaMicroConsumer, KafkaPublisher}
import com.ldaniels528.trifecta.io.zookeeper.ZKProxy
import com.ldaniels528.trifecta.messages.MessageCodecs.{LoopBackCodec, PlainTextCodec}
import com.ldaniels528.trifecta.messages.logic.Condition
import com.ldaniels528.trifecta.messages.logic.Expressions.{AND, Expression, OR}
import com.ldaniels528.trifecta.messages.query.parser.{KafkaQueryParser, KafkaQueryTokenizer}
import com.ldaniels528.trifecta.messages.{CompositeTxDecoder, MessageDecoder}
import com.ldaniels528.trifecta.rest.KafkaRestFacade._
import com.ldaniels528.trifecta.util.EitherHelper._
import com.ldaniels528.trifecta.util.OptionHelper._
import com.ldaniels528.trifecta.util.ResourceHelper._
import com.ldaniels528.trifecta.util.StringHelper._
import com.ldaniels528.trifecta.{TxConfig, TxRuntimeContext}
import kafka.common.TopicAndPartition
import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success, Try}

/**
 * Kafka REST Facade
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
case class KafkaRestFacade(config: TxConfig, zk: ZKProxy, correlationId: Int = 0) {
  private implicit val formats = net.liftweb.json.DefaultFormats
  private implicit val zkProxy: ZKProxy = zk
  private val logger = LoggerFactory.getLogger(getClass)

  // caches
  private var topicCache: Map[(String, Int), TopicDelta] = Map.empty
  private var consumerCache: Map[ConsumerDeltaKey, ConsumerJs] = Map.empty

  // define the custom thread pool
  private implicit val ec = new ExecutionContext {
    private val threadPool = Executors.newFixedThreadPool(50)

    def execute(runnable: Runnable) = {
      threadPool.submit(runnable)
      ()
    }

    def reportFailure(t: Throwable) = logger.error("Error from thread pool", t)
  }

  private val rt = TxRuntimeContext(config)(ec)

  private lazy val brokers: Seq[Broker] = KafkaMicroConsumer.getBrokerList(zk) map (b => Broker(b.host, b.port))

  private lazy val publisher = createPublisher(brokers)

  // load & register all decoders to their respective topics
  registerDecoders()

  /**
   * Registers all default decoders (found in $HOME/.trifecta/decoders) to their respective topics
   */
  def registerDecoders() {
    // register the decoders
    config.getDecoders.filter(_.decoder.isLeft).groupBy(_.topic) foreach { case (topic, decoders) =>
      rt.registerDecoder(topic, new CompositeTxDecoder(decoders))
    }

    // report all failed decoders
    config.getDecoders.filter(_.decoder.isRight) foreach { decoder =>
      decoder.decoder match {
        case Right(d) =>
          logger.error(s"Failed to compile Avro schema for topic '${decoder.topic}'. Error: ${d.error.getMessage}")
        case _ =>
      }
    }
  }

  /**
   * Executes the query represented by the JSON string
   * @param jsonString the JSON given string
   * @return the query results
   */
  def executeQuery(jsonString: String) = {
    val query = JsonHelper.transform[QueryJs](jsonString)
    val asyncIO = KafkaQueryParser(query.queryString).executeQuery(rt)
    asyncIO.task
  }

  def findOne(topic: String, criteria: String) = {
    val decoder_? = rt.lookupDecoderByName(topic)
    val conditions = parseCondition(criteria, decoder_?)

    // execute the query
    KafkaMicroConsumer.findOne(topic, brokers, forward = true, conditions) map (
      _ map { case (partition, md) => (partition, md.offset, decoder_?.map(_.decode(md.message)))
      }) map {
      case Some((partition, offset, Some(Success(message)))) =>
        MessageJs(`type` = "json", payload = message.toString, topic = Option(topic), partition = Some(partition), offset = Some(offset))
      case Some(_) =>
        throw new RuntimeException("Failed to retrieve a message")
      case None =>
        throw new RuntimeException("Failed to retrieve a message")
    }
  }

  /**
   * Parses a condition statement
   * @param expression the given expression
   * @param decoder the optional [[MessageDecoder]]
   * @example lastTrade < 1 and volume > 1000000
   * @return a collection of [[Condition]] objects
   */
  private def parseCondition(expression: String, decoder: Option[MessageDecoder[_]]): Condition = {
    import com.ldaniels528.trifecta.messages.logic.ConditionCompiler._
    import com.ldaniels528.trifecta.messages.query.parser.KafkaQueryParser.deQuote

    val it = KafkaQueryTokenizer.parse(expression).iterator
    var criteria: Option[Expression] = None
    while (it.hasNext) {
      val args = it.take(criteria.size + 3).toList
      criteria = args match {
        case List(keyword, field, operator, value) if keyword.equalsIgnoreCase("and") => criteria.map(AND(_, compile(field, operator, deQuote(value))))
        case List(keyword, field, operator, value) if keyword.equalsIgnoreCase("or") => criteria.map(OR(_, compile(field, operator, deQuote(value))))
        case List(field, operator, value) => Option(compile(field, operator, deQuote(value)))
        case unknown => throw new IllegalArgumentException(s"Illegal operand $unknown")
      }
    }
    criteria.map(compile(_, decoder)).getOrElse(throw new IllegalArgumentException(s"Invalid expression: $expression"))
  }

  /**
   * Returns the list of brokers
   * @return the JSON list of brokers
   */
  def getBrokers = Try(brokers)

  /**
   * Returns a collection of consumers that have changed since the last call
   * @return a collection of [[ConsumerJs]] objects
   */
  def getConsumerDeltas: Seq[ConsumerJs] = {
    // combine the futures for the two lists
    val consumers = Try(getConsumerGroupsNative).getOrElse(Nil) ++ Try(getConsumerGroupsPM).getOrElse(Nil)

    // extract and return only the consumers that have changed
    val deltas = if (consumerCache.isEmpty) consumers
    else {
      consumers.flatMap(c =>
        consumerCache.get(c.getKey) match {
          case Some(prev) => if (prev != c) Option(c.copy(rate = computeTransferRate(prev, c))) else None
          case None => Option(c)
        })
    }

    consumerCache = consumerCache ++ Map(consumers.map(c => c.getKey -> c): _*)
    deltas
  }


  private def computeTransferRate(a: ConsumerJs, b: ConsumerJs): Option[Double] = {
    for {
    // compute the delta of the messages
      messages0 <- a.messagesLeft
      messages1 <- b.messagesLeft
      msgDelta = (messages1 - messages0).toDouble

      // compute the time delta
      time0 <- a.lastModified
      time1 <- b.lastModified
      timeDelta = (time1 - time0).toDouble / 1000d

      // compute the rate
      rate = if (timeDelta > 0) msgDelta / timeDelta else msgDelta
    } yield rate
  }

  /**
   * Returns all consumers for all topics
   * @return a list of consumers
   */
  def getConsumers = Try {
    val consumersA = Try(getConsumerGroupsNative) match {
      case Success(v) => v
      case Failure(e) =>
        logger.error("Failed to retrieve the Kafka-native consumers", e)
        Nil
    }

    val consumersB = Try(getConsumerGroupsPM) match {
      case Success(v) => v
      case Failure(e) =>
        logger.error("Failed to retrieve the Kafka-Storm partition manager consumers", e)
        Nil
    }

    consumersA ++ consumersB
  }

  /**
   * Returns all consumers for all topics
   * @return a list of consumers
   */
  def getConsumerSet = Try {
    val consumers = getConsumerGroupsNative ++ (if (config.consumersPartitionManager) getConsumerGroupsPM else Nil)
    consumers.groupBy(_.topic) map { case (topic, consumersA) =>
      val results = (consumersA.groupBy(_.consumerId) map { case (consumerId, consumersB) =>
        ConsumerConsumerJs(consumerId, consumersB)
      }).toSeq
      ConsumerTopicJs(topic, results)
    }
  }

  /**
   * Returns the Kafka-native consumer groups
   * @return the Kafka-native consumer groups
   */
  private def getConsumerGroupsNative: Seq[ConsumerJs] = {
    KafkaMicroConsumer.getConsumerList() map { c =>
      val topicOffset = getLastOffset(c.topic, c.partition)
      val delta = topicOffset map (offset => Math.max(0L, offset - c.offset))
      ConsumerJs(c.consumerId, c.topic, c.partition, c.offset, topicOffset, c.lastModified, delta, rate = None)
    }
  }

  /**
   * Returns the Kafka Spout consumers (Partition Manager)
   * @return the Kafka Spout consumers
   */
  private def getConsumerGroupsPM: Seq[ConsumerJs] = {
    KafkaMicroConsumer.getSpoutConsumerList() map { c =>
      val topicOffset = getLastOffset(c.topic, c.partition)
      val delta = topicOffset map (offset => Math.max(0L, offset - c.offset))
      ConsumerJs(c.topologyName, c.topic, c.partition, c.offset, topicOffset, c.lastModified, delta, rate = None)
    }
  }

  /**
   * Returns a decoder by topic
   * @return the collection of decoders
   */
  def getDecoderByTopic(topic: String) = Try(toDecoderJs(topic, config.getDecodersByTopic(topic)))

  /**
   * Returns a decoder by topic and schema name
   * @return the option of a decoder
   */
  def getDecoderSchemaByName(topic: String, schemaName: String) = Try {
    val decoders = config.getDecoders.filter(_.topic == topic)
    decoders.filter(_.name == schemaName).map(_.decoder match {
      case Left(v) => v.schema.toString(true)
      case Right(v) => v.schemaString
    }).headOption.orDie(s"No decoder named '$schemaName' was found for topic $topic")
  }

  /**
   * Returns all available decoders
   * @return the collection of decoders
   */
  def getDecoders = {
    (config.getDecoders.groupBy(_.topic) map { case (topic, myDecoders) =>
      toDecoderJs(topic, myDecoders.sortBy(-_.lastModified))
    }).toSeq
  }

  private def toDecoderJs(topic: String, decoders: Seq[TxDecoder]) = {
    val schemas = decoders map { d =>
      d.decoder match {
        case Left(decoder) => SchemaJs(topic, d.name, JsonHelper.makePretty(decoder.schema.toString), error = None)
        case Right(decoder) => SchemaJs(topic, d.name, decoder.schemaString, error = Some(decoder.error.getMessage))
      }
    }
    DecoderJs(topic, schemas)
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
   * Retrieves the list of partition leaders and replicas for a given topic
   */
  def getLeaderAndReplicas(topic: String) = {
    KafkaMicroConsumer.getLeaderAndReplicas(topic, brokers) flatMap {
      case LeaderAndReplicas(partition, leader, replicas) =>
        replicas map (LeaderAndReplicasJs(partition, leader, _))
    } sortBy (_.partition)
  }

  /**
   * Retrieves the message data for given topic, partition and offset
   * @param topic the given topic
   * @param partition the given partition
   * @param offset the given offset
   * @return the JSON representation of the message
   */
  def getMessageData(topic: String, partition: Int, offset: Long) = Try {
    new KafkaMicroConsumer(TopicAndPartition(topic, partition), brokers) use { cons =>
      getDefinedOffset(cons, offset) map { o =>
        decodeMessageData(topic, cons.fetch(o)(fetchSize = DEFAULT_FETCH_SIZE).headOption)
      } getOrElse ErrorJs(message = "Offset is undefined")
    }
  }

  /**
   * Retrieves the message key for given topic, partition and offset
   * @param topic the given topic
   * @param partition the given partition
   * @param offset the given offset
   * @return the JSON representation of the message key
   */
  def getMessageKey(topic: String, partition: Int, offset: Long) = Try {
    new KafkaMicroConsumer(TopicAndPartition(topic, partition), brokers) use { cons =>
      getDefinedOffset(cons, offset) map { o =>
        cons.fetch(o)(fetchSize = DEFAULT_FETCH_SIZE).headOption map (md => toMessage(md.key))
      } getOrElse ErrorJs(message = "Offset is undefined")
    }
  }

  private def getDefinedOffset(cons: KafkaMicroConsumer, offset: Long) = {
    for {
      firstOffset <- cons.getFirstOffset
      lastOffset <- cons.getLastOffset
      adjOffset = offset match {
        case o if o < firstOffset => firstOffset
        case o if o > lastOffset => lastOffset
        case o => o
      }
    } yield adjOffset
  }

  /**
   * Sequentially tests each decoder for the given topic until one is found that will decode the given message
   * @param topic the given Kafka topic
   * @param message_? an option of a [[MessageData]]
   * @return an option of a decoded message
   */
  private def decodeMessageData(topic: String, message_? : Option[MessageData]) = {
    def decoders = config.getDecoders.filter(_.topic == topic).sortBy(-_.lastModified)
    message_? map { md =>
      decoders.foldLeft[Option[MessageJs]](None) { (result, d) =>
        result ?? attemptDecode(md.message, d)
      } getOrElse message_?.map(toMessage)
    }
  }

  /**
   * Attempts to decode the given message with the given decoder
   * @param bytes the given array of bytes
   * @param txDecoder the given [[TxDecoder]]
   * @return an option of a decoded message
   */
  private def attemptDecode(bytes: Array[Byte], txDecoder: TxDecoder) = {
    txDecoder.decoder match {
      case Left(av) =>
        av.decode(bytes) match {
          case Success(record) => Option(toMessage(record))
          case Failure(e) => None
        }
      case _ => None
    }
  }

  private def toMessage(message: Any): MessageJs = message match {
    case opt: Option[Any] => toMessage(opt.orNull)
    case bytes: Array[Byte] => MessageJs(`type` = "bytes", payload = toByteArray(bytes))
    case md: MessageData if JsonHelper.isJson(new String(md.message)) => MessageJs(`type` = "json", payload = new String(md.message))
    case md: MessageData => MessageJs(`type` = "bytes", payload = toByteArray(md.message))
    case value => MessageJs(`type` = "json", payload = JsonHelper.makePretty(String.valueOf(value)))
  }

  /**
   * Retrieves the list of available queries for the given topic
   * @param topic the given topic (e.g. "shocktrade.quotes.avro")
   * @return the list of available queries
   */
  def getQueriesByTopic(topic: String) = config.getQueriesByTopic(topic) getOrElse Nil

  /**
   * Retrieves the list of Kafka replicas for a given topic
   */
  def getReplicas(topic: String) = KafkaMicroConsumer.getReplicas(topic, brokers) sortBy (_.partition)

  /**
   * Returns a collection of topics that have changed since the last call
   * @return a collection of [[TopicDelta]] objects
   */
  def getTopicDeltas = {
    // retrieve all topic/partitions for analysis
    val topics = KafkaMicroConsumer.getTopicList(brokers) flatMap { t =>
      for {
        firstOffset <- getFirstOffset(t.topic, t.partitionId)
        lastOffset <- getLastOffset(t.topic, t.partitionId)
      } yield TopicDelta(t.topic, t.partitionId, firstOffset, lastOffset, messages = Math.max(0, lastOffset - firstOffset))
    }

    // extract and return only the topics that have changed
    val deltas = if (topicCache.isEmpty) topics
    else {
      topics.flatMap(t =>
        topicCache.get((t.topic, t.partition)) match {
          case Some(prev) => if (prev != t) Option(t) else None
          case None => Option(t)
        })
    }

    // rebuild the message cache with the latest data
    topicCache = topicCache ++ Map(topics.map(t => (t.topic -> t.partition) -> t): _*)
    deltas
  }

  def getTopics = Try(KafkaMicroConsumer.getTopicList(brokers))

  def getTopicSummaries = {
    (KafkaMicroConsumer.getTopicList(brokers).groupBy(_.topic) map { case (topic, details) =>
      // produce the partitions
      val partitions = details map { detail =>
        new KafkaMicroConsumer(TopicAndPartition(topic, detail.partitionId), brokers) use { consumer =>
          // get the start and end offsets and message count
          val startOffset = consumer.getFirstOffset
          val endOffset = consumer.getLastOffset
          val messages = for {start <- startOffset; end <- endOffset} yield Math.max(0L, end - start)

          // create the topic partition
          TopicPartitionJs(detail.partitionId, startOffset, endOffset, messages, detail.leader, detail.replicas)
        }
      }

      // get the total message count
      TopicSummaryJs(topic, partitions, totalMessages = partitions.flatMap(_.messages).sum)
    }).toSeq
  }

  def getTopicByName(topic: String) = Try(KafkaMicroConsumer.getTopicList(brokers).find(_.topic == topic))

  def getTopicDetailsByName(topic: String) = {
    KafkaMicroConsumer.getTopicPartitions(topic) map { partition =>
      new KafkaMicroConsumer(TopicAndPartition(topic, partition), brokers) use { consumer =>
        val startOffset = consumer.getFirstOffset
        val endOffset = consumer.getLastOffset
        val messages = for {start <- startOffset; end <- endOffset} yield Math.max(0L, end - start)
        TopicDetailsJs(topic, partition, startOffset, endOffset, messages)
      }
    }
  }

  def getZkData(path: String, format: String) = {
    import net.liftweb.json._

    Try {
      val decoder = codecs.get(format).orDie(s"No decoder of type '$format' was found")
      zk.read(path) map decoder.decode
    } map {
      case Some(Success(bytes: Array[Byte])) => FormattedData(`type` = BINARY, toByteArray(bytes))
      case Some(Success(js: JValue)) => FormattedData(`type` = JSON, compact(render(js)))
      case Some(Success(s: String)) => FormattedData(`type` = PLAIN_TEXT, s)
      case Some(Success(v)) => FormattedData(`type` = format, v)
      case _ => ()
    }
  }

  def getZkInfo(path: String) = Try {
    val creationTime = zk.getCreationTime(path)
    val lastModified = zk.getModificationTime(path)
    val data_? = zk.read(path)
    val size_? = data_? map (_.length)
    val formattedData_? = data_? map (bytes => FormattedData(`type` = BINARY, toByteArray(bytes)))
    ZkItemInfo(path, creationTime, lastModified, size_?, formattedData_?)
  }

  def getZkPath(parentPath: String) = {
    zk.getChildren(parentPath) map { name =>
      ZkItem(name, path = if (parentPath == "/") s"/$name" else s"$parentPath/$name")
    }
  }

  def publishMessage(topic: String, jsonString: String) = Try {
    // deserialize the JSON
    val blob = JsonHelper.transform[MessageBlobJs](jsonString)

    // publish the message
    publisher.publish(
      topic,
      key = blob.key map (key => toBinary(topic, key, blob.keyFormat)) getOrElse toDefaultBinary(blob.keyFormat),
      message = toBinary(topic, blob.message, blob.messageFormat))
  }

  /**
   * Ensures that a Kafka publisher has been created
   */
  private def createPublisher(brokers: Seq[Broker]) = {
    val publisher = KafkaPublisher(brokers)
    publisher.open()
    publisher
  }

  /**
   * Converts the given value to the specified format
   * @param value the given value
   * @param format the specified format
   * @return the binary result
   */
  private def toBinary(topic: String, value: String, format: String): Array[Byte] = {
    format match {
      case "ASCII" => value.getBytes(config.encoding)
      case "Avro" => toAvroBinary(topic, value).orDie(s"No suitable decoder found for topic $topic")
      case "JSON" => JsonHelper.makeCompact(value).getBytes(config.encoding)
      case "Hex-Notation" => CommandParser.parseDottedHex(value)
      case "EPOC" => ByteBufferUtils.longToBytes(value.toLong)
      case "UUID" => ByteBufferUtils.uuidToBytes(UUID.fromString(value))
      case _ =>
        throw new IllegalArgumentException(s"Invalid format type '$format'")
    }
  }

  /**
   * Transcodes the given JSON document into an Avro-compatible byte array
   * @param topic the given topic (e.g. "shocktrade.keystats.avro")
   * @param jsonDoc the given JSON document
   * @return an option of an Avro-compatible byte array
   */
  private def toAvroBinary(topic: String, jsonDoc: String): Option[Array[Byte]] = {
    config.getDecodersByTopic(topic).foldLeft[Option[Array[Byte]]](None) { (result, txDecoder) =>
      result ?? txDecoder.decoder.toLeftOption.flatMap { decoder =>
        logger.info(s"Testing ${decoder.label}")

        Try(AvroConversion.transcodeJsonToAvroBytes(jsonDoc, decoder.schema, config.encoding)) match {
          case Success(bytes) =>
            logger.info(s"${decoder.label} produced ${bytes.length} bytes")
            Option(bytes)
          case Failure(e) =>
            logger.warn(s"${decoder.label} failed with '${e.getMessage}'")
            None
        }
      }
    }
  }

  /**
   * Generates a default value for specified format
   * @param format the specified format
   * @return the binary result
   */
  private def toDefaultBinary(format: String): Array[Byte] = {
    format match {
      case "EPOC" => ByteBufferUtils.longToBytes(System.currentTimeMillis())
      case "UUID" => ByteBufferUtils.uuidToBytes(UUID.randomUUID())
      case _ =>
        throw new IllegalArgumentException(s"Format type '$format' cannot be automatically generated")
    }
  }

  def saveQuery(jsonString: String) = Try {
    // transform the JSON string into a query
    val query = JsonHelper.transform[QueryJs](jsonString)
    val file = new File(new File(TxConfig.queriesDirectory, query.topic), s"${query.name}.kql")
    // TODO should I add a check for new vs. replace?

    new FileOutputStream(file) use { fos =>
      fos.write(query.queryString.getBytes(config.encoding))
    }

    ErrorJs(message = "Saved", `type` = "success")
  }

  def saveSchema(jsonString: String) = Try {
    // transform the JSON string into a schema
    val schema = JsonHelper.transform[SchemaJs](jsonString)
    val decoderFile = new File(new File(TxConfig.decoderDirectory, schema.topic), schema.name)
    // TODO should I add a check for new vs. replace?

    new FileOutputStream(decoderFile) use { fos =>
      fos.write(schema.schemaString.getBytes(config.encoding))
    }

    ErrorJs(message = "Saved", `type` = "success")
  }

  /**
   * Transforms the given JSON query results into comma separated values
   * @param queryResults the given query results (as a JSON string)
   * @return a collection of comma separated values
   */
  def transformResultsToCSV(queryResults: String) = {
    def toCSV(values: List[String]): String = values.map(s => s""""$s"""").mkString(",")
    Try {
      val js = JsonHelper.toJson(queryResults)
      for {
        topic <- (js \ "topic").extractOpt[String]
        labels <- (js \ "labels").extractOpt[List[String]]
        values <- (js \ "values").extractOpt[List[Map[String, String]]]
        rows = values map (m => labels map (m.getOrElse(_, "")))
      } yield toCSV(labels) :: (rows map toCSV)
    }
  }

  private def toByteArray(bytes: Array[Byte], columns: Int = 20): Seq[Seq[String]] = {
    def toHex(b: Byte): String = f"$b%02x"
    def toAscii(b: Byte): String = if (b >= 32 && b <= 127) b.toChar.toString else "."

    bytes.sliding(columns, columns).toSeq map { chunk =>
      val hexRow = chunk.map(toHex).mkString(".")
      val asciiRow = chunk.map(toAscii).mkString
      Seq(hexRow, asciiRow)
    }
  }

}

/**
 * Kafka Web Facade Singleton
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
object KafkaRestFacade {
  // Formatting Constants
  val AUTO = "auto"
  val BINARY = "binary"
  val JSON = "json"
  val PLAIN_TEXT = "plain-text"

  private val codecs = Map[String, MessageDecoder[_ <: AnyRef]](
    AUTO -> AutoDecoder, BINARY -> LoopBackCodec, PLAIN_TEXT -> PlainTextCodec, JSON -> JsonDecoder)

  /**
   * Automatic Type-Sensing Message Decoder
   */
  object AutoDecoder extends MessageDecoder[AnyRef] {

    /**
     * Decodes the binary message into a typed object
     * @param message the given binary message
     * @return a decoded message wrapped in a Try-monad
     */
    override def decode(message: Array[Byte]): Try[AnyRef] = {
      if (message.isPrintable) {
        val jsonDecoding = JsonDecoder.decode(message)
        if (jsonDecoding.isSuccess) jsonDecoding else PlainTextCodec.decode(message)
      }
      else LoopBackCodec.decode(message)
    }
  }

  case class ConsumerJs(consumerId: String, topic: String, partition: Int, offset: Long, topicOffset: Option[Long], lastModified: Option[Long], messagesLeft: Option[Long], rate: Option[Double]) {

    def getKey = ConsumerDeltaKey(consumerId, topic, partition)

  }

  case class ConsumerDeltaKey(consumerId: String, topic: String, partition: Int)

  case class ConsumerTopicJs(topic: String, consumers: Seq[ConsumerConsumerJs])

  case class ConsumerConsumerJs(consumerId: String, details: Seq[ConsumerJs])

  case class DecoderJs(topic: String, schemas: Seq[SchemaJs])

  case class SchemaJs(topic: String, name: String, schemaString: String, error: Option[String] = None)

  case class ErrorJs(message: String, `type`: String = "error")

  case class FormattedData(`type`: String, value: Any)

  case class LeaderAndReplicasJs(partition: Int, leader: Broker, replica: Broker)

  case class MessageBlobJs(key: Option[String], message: String, keyFormat: String, messageFormat: String)

  case class MessageJs(`type`: String, payload: Any, topic: Option[String] = None, partition: Option[Int] = None, offset: Option[Long] = None)

  case class QueryJs(name: String, topic: String, queryString: String)

  case class TopicDetailsJs(topic: String, partition: Int, startOffset: Option[Long], endOffset: Option[Long], messages: Option[Long])

  case class TopicDelta(topic: String, partition: Int, startOffset: Long, endOffset: Long, messages: Long)

  case class TopicSummaryJs(topic: String, partitions: Seq[TopicPartitionJs], totalMessages: Long)

  case class TopicPartitionJs(partition: Int, startOffset: Option[Long], endOffset: Option[Long], messages: Option[Long], leader: Option[Broker], replicas: Seq[Broker])

  case class ZkItem(name: String, path: String)

  case class ZkItemInfo(path: String, creationTime: Option[Long], lastModified: Option[Long], size: Option[Int], data: Option[FormattedData])

}
