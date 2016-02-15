package com.github.ldaniels528.trifecta.controllers

import java.io.{File, FileOutputStream}
import java.util.UUID

import com.github.ldaniels528.trifecta.TxConfig.TxDecoder
import com.github.ldaniels528.trifecta.command.parser.CommandParser
import com.github.ldaniels528.trifecta.controllers.KafkaRestFacade._
import com.github.ldaniels528.trifecta.io.ByteBufferUtils
import com.github.ldaniels528.trifecta.io.avro.AvroConversion
import com.github.ldaniels528.trifecta.io.json.{JsonDecoder, JsonHelper}
import com.github.ldaniels528.trifecta.io.kafka.KafkaMicroConsumer.{DEFAULT_FETCH_SIZE, MessageData}
import com.github.ldaniels528.trifecta.io.kafka.{Broker, KafkaMicroConsumer, KafkaPublisher}
import com.github.ldaniels528.trifecta.io.zookeeper.ZKProxy
import com.github.ldaniels528.trifecta.messages.MessageCodecs.{LoopBackCodec, PlainTextCodec}
import com.github.ldaniels528.trifecta.messages.logic.Condition
import com.github.ldaniels528.trifecta.messages.logic.Expressions.{AND, Expression, OR}
import com.github.ldaniels528.trifecta.messages.query.parser.{KafkaQueryParser, KafkaQueryTokenizer}
import com.github.ldaniels528.trifecta.messages.{CompositeTxDecoder, MessageDecoder}
import com.github.ldaniels528.trifecta.models.BrokerDetailsJs._
import com.github.ldaniels528.trifecta.models.BrokerJs._
import com.github.ldaniels528.trifecta.models.ConsumerDetailJs.ConsumerDeltaKey
import com.github.ldaniels528.trifecta.models.QueryDetailsJs._
import com.github.ldaniels528.trifecta.models.TopicDetailsJs._
import com.github.ldaniels528.trifecta.models._
import com.github.ldaniels528.trifecta.{TxConfig, TxRuntimeContext}
import com.github.ldaniels528.commons.helpers.EitherHelper._
import com.github.ldaniels528.commons.helpers.OptionHelper.Risky._
import com.github.ldaniels528.commons.helpers.OptionHelper._
import com.github.ldaniels528.commons.helpers.ResourceHelper._
import com.github.ldaniels528.commons.helpers.StringHelper._
import kafka.common.TopicAndPartition
import play.api.Logger
import play.api.libs.json.{JsString, Json}

import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

/**
  * Kafka REST Facade
  * @author lawrence.daniels@gmail.com
  */
case class KafkaRestFacade(config: TxConfig, zk: ZKProxy) {
  private implicit val formats = net.liftweb.json.DefaultFormats
  private implicit val zkProxy: ZKProxy = zk

  // caches
  private val topicCache = TrieMap[(String, Int), TopicDeltaWithTotalsJs]()
  private val consumerCache = TrieMap[ConsumerDeltaKey, ConsumerDetailJs]()
  private val cachedRuntime = TrieMap[Unit, TxRuntimeContext]()
  private val cachedBrokers = TrieMap[Unit, Seq[Broker]]()
  private val cachedPublisher = TrieMap[Unit, KafkaPublisher]()

  // set user defined Kafka root directory
  KafkaMicroConsumer.rootKafkaPath = config.kafkaRootPath

  // load & register all decoders to their respective topics
  def init(implicit ec: ExecutionContext) = registerDecoders()

  /**
    * Registers all default decoders (found in $HOME/.trifecta/decoders) to their respective topics
    */
  private def registerDecoders()(implicit ec: ExecutionContext) {
    // register the decoders
    config.getDecoders.filter(_.decoder.isLeft).groupBy(_.topic) foreach { case (topic, decoders) =>
      rt.registerDecoder(topic, new CompositeTxDecoder(decoders))
    }

    // report all failed decoders
    config.getDecoders.filter(_.decoder.isRight) foreach { decoder =>
      decoder.decoder match {
        case Right(d) =>
          Logger.error(s"Failed to compile Avro schema for topic '${decoder.topic}'. Error: ${d.error.getMessage}")
        case _ =>
      }
    }
  }

  /**
    * Executes the query represented by the JSON string
    * @param query the [[QueryJs query]]
    * @return the query results
    */
  def executeQuery(query: QueryJs)(implicit ec: ExecutionContext) = {
    val asyncIO = KafkaQueryParser(query.queryString).executeQuery(rt)
    asyncIO.task
  }

  def findOne(topic: String, criteria: String)(implicit ec: ExecutionContext) = {
    val decoder_? = rt.lookupDecoderByName(topic)
    val conditions = parseCondition(criteria, decoder_?)

    // execute the query
    KafkaMicroConsumer.findOne(topic, brokers, forward = true, conditions) map (
      _ map { case (partition, md) => (partition, md.offset, decoder_?.map(_.decode(md.message)))
      }) map {
      case Some((partition, offset, Some(Success(message)))) =>
        MessageJs(`type` = "json", payload = Json.parse(message.toString), topic = Option(topic), partition = Some(partition), offset = Some(offset))
      case Some(_) =>
        throw new RuntimeException("Failed to retrieve a message")
      case None =>
        throw new RuntimeException("Failed to retrieve a message")
    }
  }

  /**
    * Returns the promise of the option of a message based on the given search criteria
    * @param c the given [[SamplingCursor]]
    * @return the promise of the option of a message based on the given search criteria
    */
  def findNext(c: SamplingCursor, attempts: Int = 0)(implicit ec: ExecutionContext): Option[MessageJs] = {
    // attempt to find at least one offset with messages available
    c.offsets.find(co => co.consumerOffset.exists(c => co.topicOffset.exists(t => c <= t))) flatMap { offset =>
      new KafkaMicroConsumer(TopicAndPartition(c.topic, offset.partition), brokers) use { subs =>
        val message = for {
          consumerOfs <- offset.consumerOffset
          message <- subs.fetch(consumerOfs)(fetchSize = 65535).headOption
        } yield message

        message.foreach(_ => offset.consumerOffset = offset.consumerOffset.map(_ + 1))
        decodeMessageData(c.topic, message)
      }
    }
  }

  def createSamplingCursor(request: MessageSamplingStartRequest)(implicit ec: ExecutionContext) = {
    val cursorOffsets = request.partitionOffsets.indices zip request.partitionOffsets map { case (partition, offset) =>
      new KafkaMicroConsumer(TopicAndPartition(request.topic, partition), brokers) use { consumer =>
        SamplingCursorOffsets(
          partition = partition,
          topicOffset = consumer.getLastOffset,
          consumerOffset = consumer.getFirstOffset.map(Math.max(offset, _)))
      }
    } sortBy (c => -(c.topicOffset.getOrElse(0L) - c.consumerOffset.getOrElse(0L)))
    SamplingCursor(request.topic, cursorOffsets)
  }

  /**
    * Parses a condition statement
    * @param expression the given expression
    * @param decoder the optional [[MessageDecoder]]
    * @example lastTrade < 1 and volume > 1000000
    * @return a collection of [[Condition]] objects
    */
  private def parseCondition(expression: String, decoder: Option[MessageDecoder[_]]): Condition = {
    import com.github.ldaniels528.trifecta.messages.logic.ConditionCompiler._
    import com.github.ldaniels528.trifecta.messages.query.parser.KafkaQueryParser.deQuote

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
  def getBrokers(implicit ec: ExecutionContext) = brokers.map(_.asJson)

  /**
    * Returns the list of brokers
    * @return the JSON list of brokers
    */
  def getBrokerDetails = KafkaMicroConsumer.getBrokerList.groupBy(_.host) map { case (host, details) => BrokerDetailsGroupJs(host, details.map(_.asJson)) }

  /**
    * Returns a collection of consumers that have changed since the last call
    * @return a promise of a collection of [[ConsumerDetailJs]]
    */
  def getConsumerDeltas(implicit ec: ExecutionContext): Seq[ConsumerDetailJs] = {
    val consumers = getConsumerDetails

    // extract and return only the consumers that have changed
    val deltas = if (consumerCache.isEmpty) consumers
    else {
      consumers.flatMap { c =>
        consumerCache.get(c.getKey) match {
          // Option(c.copy(rate = computeTransferRate(prev, c)))
          case Some(prev) => if (prev != c) Option(c) else None
          case None => Option(c)
        }
      }
    }

    // update the cache
    deltas.foreach { c =>
      consumerCache(c.getKey) = c
    }

    deltas
  }

  private def computeTransferRate(a: ConsumerDetailJs, b: ConsumerDetailJs): Option[Double] = {
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
    * Returns the consumer details for all topics
    * @return a list of consumers
    */
  def getConsumerDetails(implicit ec: ExecutionContext): Seq[ConsumerDetailJs] = {
    getConsumerGroupsNative() ++ (if (config.consumersPartitionManager) getConsumerGroupsPM else Nil)
  }

  def getConsumersByTopic(topic: String)(implicit ec: ExecutionContext) = {
    getConsumerDetails
      .filter(_.topic == topic)
      .groupBy(_.consumerId)
      .map { case (consumerId, details) => ConsumerByTopicJs(consumerId, details) }
  }

  /**
    * Returns the Kafka-native consumer groups
    * @return the Kafka-native consumer groups
    */
  private def getConsumerGroupsNative(topicPrefix: Option[String] = None)(implicit ec: ExecutionContext): Seq[ConsumerDetailJs] = {
    KafkaMicroConsumer.getConsumerDetails() map { c =>
      val topicOffset = Try(getLastOffset(c.topic, c.partition)) getOrElse None
      val delta = topicOffset map (offset => Math.max(0L, offset - c.offset))
      ConsumerDetailJs(c.consumerId, c.topic, c.partition, c.offset, topicOffset, c.lastModified, delta, rate = None)
    }
  }

  /**
    * Returns the Kafka Spout consumers (Partition Manager)
    * @return the Kafka Spout consumers
    */
  private def getConsumerGroupsPM(implicit ec: ExecutionContext): Seq[ConsumerDetailJs] = {
    KafkaMicroConsumer.getStormConsumerList() map { c =>
      val topicOffset = getLastOffset(c.topic, c.partition)
      val delta = topicOffset map (offset => Math.max(0L, offset - c.offset))
      ConsumerDetailJs(c.topologyName, c.topic, c.partition, c.offset, topicOffset, c.lastModified, delta, rate = None)
    }
  }

  /**
    * Returns a decoder by topic
    * @return the collection of decoders
    */
  def getDecoderByTopic(topic: String) = toDecoderJs(topic, config.getDecodersByTopic(topic))

  /**
    * Returns a decoder by topic and schema name
    * @return the option of a decoder
    */
  def getDecoderSchemaByName(topic: String, schemaName: String) = {
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
  def getFirstOffset(topic: String, partition: Int)(implicit zk: ZKProxy, ec: ExecutionContext): Option[Long] = {
    new KafkaMicroConsumer(TopicAndPartition(topic, partition), brokers) use (_.getFirstOffset)
  }

  /**
    * Returns the last offset for a given topic
    */
  def getLastOffset(topic: String, partition: Int)(implicit zk: ZKProxy, ec: ExecutionContext): Option[Long] = {
    new KafkaMicroConsumer(TopicAndPartition(topic, partition), brokers) use (_.getLastOffset)
  }

  /**
    * Retrieves the message data for given topic, partition and offset
    * @param topic the given topic
    * @param partition the given partition
    * @param offset the given offset
    * @return the JSON representation of the message
    */
  def getMessageData(topic: String, partition: Int, offset: Long)(implicit ec: ExecutionContext) = {
    new KafkaMicroConsumer(TopicAndPartition(topic, partition), brokers) use { cons =>
      getDefinedOffset(cons, offset) flatMap { theOffset =>
        decodeMessageData(topic, cons.fetch(theOffset)(fetchSize = DEFAULT_FETCH_SIZE).headOption)
      } getOrElse {
        MessageJs(`type` = "error", message = "Offset is undefined")
      }
    }
  }

  /**
    * Retrieves the message key for given topic, partition and offset
    * @param topic the given topic
    * @param partition the given partition
    * @param offset the given offset
    * @return the JSON representation of the message key
    */
  def getMessageKey(topic: String, partition: Int, offset: Long)(implicit ec: ExecutionContext) = {
    new KafkaMicroConsumer(TopicAndPartition(topic, partition), brokers) use { cons =>
      getDefinedOffset(cons, offset) flatMap { o =>
        cons.fetch(o)(fetchSize = DEFAULT_FETCH_SIZE).headOption map (md => toMessage(md.key))
      } getOrElse MessageJs(`type` = "error", message = "Offset is undefined")
    }
  }

  private def getDefinedOffset(cons: KafkaMicroConsumer, offset: Long): Option[Long] = {
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
  private def decodeMessageData(topic: String, message_? : Option[MessageData]): Option[MessageJs] = {
    def decoders = config.getDecodersByTopic(topic)
    message_? flatMap { md =>
      decoders.foldLeft[Option[MessageJs]](None) { (result, d) =>
        result ?? attemptDecode(md, d)
      } ?? message_?.map(toMessage)
    }
  }

  /**
    * Attempts to decode the given message with the given decoder
    * @param md the given [[MessageData]]
    * @param txDecoder the given [[TxDecoder]]
    * @return an option of a decoded message
    */
  private def attemptDecode(md: MessageData, txDecoder: TxDecoder): Option[MessageJs] = {
    txDecoder.decoder match {
      case Left(av) =>
        av.decode(md.message) match {
          case Success(record) =>
            Option(MessageJs(`type` = "json", partition = md.partition, offset = md.offset, payload = Json.parse(record.toString)))
          case Failure(e) => None
        }
      case _ => None
    }
  }

  private def toMessage(message: Any): MessageJs = message match {
    case opt: Option[Any] => toMessage(opt.orNull)
    case bytes: Array[Byte] =>
      MessageJs(`type` = "bytes", payload = toByteArray(bytes))
    case md: MessageData if JsonHelper.isJson(new String(md.message)) =>
      MessageJs(`type` = "json", partition = md.partition, offset = md.offset, payload = Json.parse(new String(md.message)))
    case md: MessageData =>
      MessageJs(`type` = "bytes", partition = md.partition, offset = md.offset, payload = toByteArray(md.message))
    case value =>
      MessageJs(`type` = "json", payload = Json.parse(value.toString))
  }

  /**
    * Retrieves the list of available queries for the any topic
    * @return the list of available queries
    */
  def getQueries = config.getQueries.map(_.map(_.asJson)) getOrElse Nil

  /**
    * Retrieves the list of available queries for the given topic
    * @param topic the given topic (e.g. "shocktrade.quotes.avro")
    * @return the list of available queries
    */
  def getQueriesByTopic(topic: String) = {
    config.getQueriesByTopic(topic).map(_.map(_.asJson)) getOrElse Nil
  }

  /**
    * Retrieves the list of Kafka replicas for a given topic
    */
  def getReplicas(topic: String)(implicit ec: ExecutionContext) = {
    KafkaMicroConsumer.getReplicas(topic, brokers)
      .map(r => (r.partition, ReplicaHostJs(s"${r.host}:${r.port}", r.inSync)))
      .groupBy(_._1)
      .map { case (partition, replicas) => ReplicaJs(partition, replicas.map(_._2)) }
  }

  /**
    * Returns a collection of topics that have changed since the last call
    * @return a collection of [[TopicDeltaWithTotalsJs deltas]]
    */
  def getTopicDeltas(implicit ec: ExecutionContext) = {
    // retrieve all topic/partitions for analysis
    val topics = KafkaMicroConsumer.getTopicList(brokers) flatMap { t =>
      for {
        firstOffset <- getFirstOffset(t.topic, t.partitionId)
        lastOffset <- getLastOffset(t.topic, t.partitionId)
      } yield TopicDeltaJs(topic = t.topic, partition = t.partitionId, startOffset = firstOffset, endOffset = lastOffset, messages = Math.max(0, lastOffset - firstOffset))
    }

    // get the total message counts
    val totalMessageCounts = topics.groupBy(_.topic) map { case (topic, info) => (topic, info.map(_.messages).sum) }
    val topicsWithCounts = topics map { t =>
      TopicDeltaWithTotalsJs(t.topic, t.partition, t.startOffset, t.endOffset, t.messages, totalMessageCounts(t.topic))
    }

    // extract and return only the topics that have changed
    val deltas = if (topicCache.isEmpty) topicsWithCounts
    else {
      topicsWithCounts.flatMap(t => topicCache.get((t.topic, t.partition)) match {
        case Some(prev) => if (prev != t) Option(t) else None
        case None => Option(t)
      })
    }

    // update the cache
    deltas.foreach { t =>
      topicCache(t.topic -> t.partition) = t
    }

    deltas
  }

  def getTopics(implicit ec: ExecutionContext) = Future(KafkaMicroConsumer.getTopicList(brokers).map(_.asJson))

  def getTopicSummaries(implicit ec: ExecutionContext) = Future {
    (KafkaMicroConsumer.getTopicList(brokers).groupBy(_.topic) map { case (topic, details) =>
      // produce the partitions
      val partitions = details map { detail =>
        new KafkaMicroConsumer(TopicAndPartition(topic, detail.partitionId), brokers) use { consumer =>
          // get the start and end offsets and message count
          val startOffset = consumer.getFirstOffset
          val endOffset = consumer.getLastOffset
          val messages = for {start <- startOffset; end <- endOffset} yield Math.max(0L, end - start)

          // create the topic partition
          TopicPartitionJs(detail.partitionId, startOffset, endOffset, messages, detail.leader.map(_.asJson), detail.replicas.map(_.asJson))
        }
      }

      // get the total message count
      TopicSummaryJs(topic, partitions, totalMessages = partitions.flatMap(_.messages).sum)
    }).toSeq
  }

  def getTopicByName(topic: String)(implicit ec: ExecutionContext) = {
    KafkaMicroConsumer.getTopicList(brokers).find(_.topic == topic).map(_.asJson)
  }

  def getTopicDetailsByName(topic: String)(implicit ec: ExecutionContext) = {
    KafkaMicroConsumer.getTopicPartitions(topic) map { partition =>
      new KafkaMicroConsumer(TopicAndPartition(topic, partition), brokers) use { consumer =>
        val startOffset = consumer.getFirstOffset
        val endOffset = consumer.getLastOffset
        val messages = for {start <- startOffset; end <- endOffset} yield Math.max(0L, end - start)
        TopicDetailsJs(topic, partition, startOffset, endOffset, messages)
      }
    }
  }

  def getZkData(path: String, format: String)(implicit ec: ExecutionContext) = {
    val decoder = codecs.get(format).orDie(s"No decoder of type '$format' was found")
    zk.read(path) map decoder.decode match {
      case Some(Success(bytes: Array[Byte])) => Option(FormattedDataJs(`type` = BINARY, toByteArray(bytes)))
      case Some(Success(js: net.liftweb.json.JValue)) => Option(FormattedDataJs(`type` = JSON, Json.parse(net.liftweb.json.compactRender(js))))
      case Some(Success(s: String)) => Option(FormattedDataJs(`type` = PLAIN_TEXT, JsString(s)))
      case Some(Success(v)) => Option(FormattedDataJs(`type` = format, JsString(v.toString))) // TODO detect type
      case _ => None
    }
  }

  def getZkInfo(path: String)(implicit ec: ExecutionContext) = {
    val creationTime = zk.getCreationTime(path)
    val lastModified = zk.getModificationTime(path)
    val data_? = zk.read(path)
    val size_? = data_? map (_.length)
    val formattedData_? = data_? map (bytes => FormattedDataJs(`type` = BINARY, toByteArray(bytes)))
    ZkItemInfoJs(path, creationTime, lastModified, size_?, formattedData_?)
  }

  def getZkPath(parentPath: String)(implicit ec: ExecutionContext) = {
    zk.getChildren(parentPath) map { name =>
      ZkItemJs(name, path = if (parentPath == "/") s"/$name" else s"$parentPath/$name")
    }
  }

  def publishMessage(topic: String, jsonString: String)(implicit ec: ExecutionContext) = {
    // deserialize the JSON
    val blob = JsonHelper.transform[MessageBlobJs](jsonString)

    // publish the message
    val publisher = cachedPublisher.getOrElseUpdate((), createPublisher(brokers))
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
      case "JSON" => JsonHelper.compressJson(value).getBytes(config.encoding)
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
        Logger.info(s"Testing ${decoder.label}")

        Try(AvroConversion.transcodeJsonToAvroBytes(jsonDoc, decoder.schema, config.encoding)) match {
          case Success(bytes) =>
            Logger.info(s"${decoder.label} produced ${bytes.length} bytes")
            Option(bytes)
          case Failure(e) =>
            Logger.warn(s"${decoder.label} failed with '${e.getMessage}'")
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

  def saveDecoderSchema(schema: SchemaJs) = {
    val schemaFile = new File(new File(TxConfig.decoderDirectory, schema.topic), getNameWithExtension(schema.name, ".avsc"))
    // TODO should I add a check for new vs. replace?

    // ensure the parent directory exists
    createParentDirectory(schemaFile)

    // save the schema file to disk
    new FileOutputStream(schemaFile) use { fos =>
      fos.write(schema.schemaString.getBytes(config.encoding))
    }

    MessageJs(message = "Saved", `type` = "success")
  }

  def saveQuery(query: QueryDetailsJs)(implicit ec: ExecutionContext) = Future {
    val queryFile = new File(new File(TxConfig.queriesDirectory, query.topic), getNameWithExtension(query.name, ".kql"))
    // TODO should I add a check for new vs. replace?

    // ensure the parent directory exists
    createParentDirectory(queryFile)

    // save the query file to disk
    new FileOutputStream(queryFile) use { fos =>
      fos.write(query.queryString.getBytes(config.encoding))
    }

    MessageJs(message = "Saved", `type` = "success")
  }

  private def createParentDirectory(file: File) {
    val parentDirectory = file.getParentFile
    if (!parentDirectory.exists) {
      Logger.info(s"Creating directory '${parentDirectory.getAbsolutePath}'...")
      if (!parentDirectory.mkdirs()) {
        Logger.warn(s"Directory '${parentDirectory.getAbsolutePath}' could not be created")
      }
    }
  }

  private def getNameWithExtension(name: String, extension: String): String = {
    name.lastIndexOptionOf(extension) match {
      case Some(_) => name
      case None => name + extension
    }
  }

  /**
    * Transforms the given JSON query results into comma separated values
    * @param queryResults the given query results (as a JSON string)
    * @return a collection of comma separated values
    */
  def transformResultsToCSV(queryResults: String)(implicit ec: ExecutionContext) = {
    def toCSV(values: List[String]): String = values.map(s => s""""$s"""").mkString(",")
    Future {
      val js = JsonHelper.toJson(queryResults)
      for {
        topic <- (js \ "topic").extractOpt[String]
        labels <- (js \ "labels").extractOpt[List[String]]
        values <- (js \ "values").extractOpt[List[Map[String, String]]]
        rows = values map (m => labels map (m.getOrElse(_, "")))
      } yield toCSV(labels) :: (rows map toCSV)
    }
  }

  private def brokers(implicit ec: ExecutionContext): Seq[Broker] = {
    cachedBrokers.getOrElseUpdate((), KafkaMicroConsumer.getBrokerList map (b => Broker(b.host, b.port)))
  }

  private def rt(implicit ec: ExecutionContext) = {
    cachedRuntime.getOrElseUpdate((), TxRuntimeContext(config))
  }

  private def toByteArray(bytes: Array[Byte], columns: Int = 20) = {
    def toHex(b: Byte): String = f"$b%02x"
    def toAscii(b: Byte): String = if (b >= 32 && b <= 127) b.toChar.toString else "."

    val data = bytes.sliding(columns, columns).toSeq map { chunk =>
      val hexRow = chunk.map(toHex).mkString(".")
      val asciiRow = chunk.map(toAscii).mkString
      Seq(hexRow, asciiRow)
    }
    Json.toJson(data)
  }

}

/**
  * Kafka Web Facade Singleton
  * @author lawrence.daniels@gmail.com
  */
object KafkaRestFacade {
  // Formatting Constants
  val AUTO = "auto"
  val BINARY = "binary"
  val JSON = "json"
  val PLAIN_TEXT = "plain-text"

  private val codecs = Map[String, MessageDecoder[_ <: AnyRef]](
    AUTO -> AutoDecoder,
    BINARY -> LoopBackCodec,
    PLAIN_TEXT -> PlainTextCodec,
    JSON -> JsonDecoder)

}
