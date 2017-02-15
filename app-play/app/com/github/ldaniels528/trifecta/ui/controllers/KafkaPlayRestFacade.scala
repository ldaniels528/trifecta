package com.github.ldaniels528.trifecta.ui.controllers

import java.io.File
import java.util.UUID

import _root_.kafka.common.TopicAndPartition
import com.github.ldaniels528.commons.helpers.OptionHelper.Risky._
import com.github.ldaniels528.commons.helpers.OptionHelper._
import com.github.ldaniels528.commons.helpers.ResourceHelper._
import com.github.ldaniels528.commons.helpers.StringHelper._
import com.github.ldaniels528.trifecta.TxConfig.{TxDecoder, TxFailedSchema, TxSuccessSchema}
import com.github.ldaniels528.trifecta.io.kafka.KafkaMicroConsumer.{DEFAULT_FETCH_SIZE, MessageData}
import com.github.ldaniels528.trifecta.io.kafka.KafkaZkUtils.{ConsumerGroup, ConsumerOffset}
import com.github.ldaniels528.trifecta.io.kafka._
import com.github.ldaniels528.trifecta.io.zookeeper.ZKProxy
import com.github.ldaniels528.trifecta.io.{IOCounter, _}
import com.github.ldaniels528.trifecta.messages._
import com.github.ldaniels528.trifecta.messages.codec.MessageCodecFactory.{LoopBackCodec, PlainTextCodec}
import com.github.ldaniels528.trifecta.messages.codec.avro.{AvroConversion, AvroDecoder}
import com.github.ldaniels528.trifecta.messages.codec.json.{JsonHelper, JsonMessageDecoder, JsonTransCoding}
import com.github.ldaniels528.trifecta.messages.codec.{CompositeMessageDecoder, MessageDecoder}
import com.github.ldaniels528.trifecta.messages.logic.ConditionCompiler
import com.github.ldaniels528.trifecta.messages.query.KQLResult
import com.github.ldaniels528.trifecta.messages.query.parser.{KafkaQueryParser, KafkaQueryTokenizer}
import com.github.ldaniels528.trifecta.ui.controllers.KafkaPlayRestFacade._
import com.github.ldaniels528.trifecta.ui.models.BrokerDetailsJs._
import com.github.ldaniels528.trifecta.ui.models.BrokerJs._
import com.github.ldaniels528.trifecta.ui.models.ConsumerDetailJs._
import com.github.ldaniels528.trifecta.ui.models.QueryDetailsJs._
import com.github.ldaniels528.trifecta.ui.models.TopicDetailsJs._
import com.github.ldaniels528.trifecta.ui.models._
import com.github.ldaniels528.trifecta.util.ParsingHelper
import com.github.ldaniels528.trifecta.{TxConfig, TxRuntimeContext}
import org.apache.kafka.clients.producer.RecordMetadata
import play.api.Logger
import play.api.libs.json.{JsString, Json}

import scala.collection.concurrent.TrieMap
import scala.collection.immutable.Iterable
import scala.concurrent.{ExecutionContext, Future, blocking}
import scala.language.postfixOps
import scala.util.{Failure, Success, Try}

/**
  * Kafka Play REST Facade
  * @author lawrence.daniels@gmail.com
  */
case class KafkaPlayRestFacade(config: TxConfig, zk: ZKProxy) {
  private implicit val formats = net.liftweb.json.DefaultFormats
  private implicit val zkProxy: ZKProxy = zk

  // caches
  private val cachedBrokers = TrieMap[Unit, Seq[Broker]]()
  private val cachedDeltaConsumers = TrieMap[(String, String, Int), ConsumerOffset]()
  private val cachedDeltaTopics = TrieMap[(String, Int), TopicDeltaJs]()
  private val cachedRuntime = TrieMap[Unit, TxRuntimeContext]()
  private val cachedPublisher = TrieMap[Unit, KafkaPublisher]()

  // set user defined Kafka root directory
  KafkaMicroConsumer.kafkaUtil.rootKafkaPath = config.kafkaRootPath

  // load & register all decoders to their respective topics
  def init(implicit ec: ExecutionContext): Unit = {
    registerDecoders()
  }

  /**
    * Registers all default decoders (found in $HOME/.trifecta/decoders) to their respective topics
    */
  private def registerDecoders()(implicit ec: ExecutionContext) {
    // register the decoders
    config.getDecoders.filter(_.decoder.isSuccess).groupBy(_.topic) foreach { case (topic, decoders) =>
      rt.registerDecoder(topic, new CompositeMessageDecoder(decoders))
    }

    // report all failed decoders
    config.getDecoders.filterNot(_.decoder.isSuccess) foreach { decoder =>
      decoder.decoder match {
        case TxFailedSchema(_, error, _) =>
          Logger.error(s"Failed to compile Avro schema for topic '${decoder.topic}'. Error: ${error.getMessage}")
        case _ =>
      }
    }
  }

  /**
    * Executes the query represented by the JSON string
    * @param query the [[QueryRequestJs query]]
    * @return the query results
    */
  def executeQuery(query: QueryRequestJs)(implicit ec: ExecutionContext): Future[KQLResult] = {
    val counter = IOCounter(System.currentTimeMillis())
    KafkaQueryParser(query.queryString).executeQuery(rt, counter)
  }

  def findOne(topic: String, criteria: String)(implicit ec: ExecutionContext): Future[MessageJs] = {
    val decoder_? = rt.lookupDecoderByName(topic)
    val it = KafkaQueryTokenizer.parse(criteria).iterator
    val conditions = ConditionCompiler.parseCondition(it, decoder_?)
      .getOrElse(throw new IllegalArgumentException(s"Invalid expression: $criteria"))

    // execute the query
    KafkaMicroConsumer.findOne(topic, brokers, conditions) map (
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
        decodeMessageData(c.topic, message, decode = true) // TODO revisit this
      }
    }
  }

  def createSamplingCursor(request: MessageSamplingStartRequest)(implicit ec: ExecutionContext): SamplingCursor = {
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
    * Returns the list of brokers
    * @return the JSON list of brokers
    */
  def getBrokers: Seq[BrokerJs] = brokers.map(_.asJson)

  /**
    * Returns the list of brokers
    * @return the JSON list of brokers
    */
  def getBrokerDetails: Iterable[BrokerDetailsGroupJs] = {
    KafkaMicroConsumer.getBrokerList.groupBy(_.host) map { case (host, details) => BrokerDetailsGroupJs(host, details.map(_.asJson)) }
  }

  /**
    * Returns a collection of consumers that have changed since the last call
    * @return a promise of a collection of [[ConsumerDetailJs]]
    */
  def getConsumerDeltas(implicit ec: ExecutionContext): Seq[ConsumerOffsetUpdateJs] = {
    val consumers = getConsumerGroupsNativeOffsets() ++ getConsumerGroupsZookeeperOffsets
    val firstCall = cachedDeltaConsumers.isEmpty

    // extract and return only the consumers that have changed
    val deltas = if (cachedDeltaConsumers.isEmpty) consumers
    else {
      consumers.flatMap { c =>
        cachedDeltaConsumers.get((c.groupId, c.topic, c.partition)) match {
          // Option(c.copy(rate = computeTransferRate(prev, c)))
          case Some(prev) => if (prev != c) Option(c) else None
          case None => Option(c)
        }
      }
    }

    // update the cache
    deltas.foreach { c =>
      cachedDeltaConsumers((c.groupId, c.topic, c.partition)) = c
    }

    // if the cache was previously empty, send nothing
    if(firstCall) Nil
    else {
      deltas map { o =>
        ConsumerOffsetUpdateJs(
          consumerId = o.groupId,
          topic = o.topic,
          partition = o.partition,
          offset = o.offset,
          lastModifiedTime = o.lastModifiedTime)
      }
    }
  }

  def getConsumerGroup(groupId: String): Option[ConsumerGroupJs] = {
    if (config.getConsumerGroupList.contains(groupId))
      getConsumerGroupNative(groupId)
    else
      getConsumerGroupZookeeper(groupId)
  }

  def getConsumerGroupsZookeeperOffsets: Seq[ConsumerOffset] = {
    KafkaMicroConsumer.kafkaUtil.getConsumerGroupIds.flatMap { groupId =>
      Try(KafkaMicroConsumer.kafkaUtil.getConsumerOffsets(groupId)) getOrElse Nil
    }
  }

  def getConsumerGroupZookeeper(groupId: String): Option[ConsumerGroupJs] = {
    KafkaMicroConsumer.kafkaUtil.getConsumerGroup(groupId) map { cg =>
      ConsumerGroupJs(
        consumerId = cg.consumerId,
        offsets = cg.offsets.map { o =>
          val limits = getLimitOffsets(o.topic, o.partition)
          ConsumerOffsetJs(
            groupId = groupId,
            topic = o.topic,
            partition = o.partition,
            offset = o.offset,
            topicStartOffset = limits.map(_._1),
            topicEndOffset = limits.map(_._2),
            messages = limits map { case (start, end) => Math.max(end - start, 0L) },
            lastModifiedTime = o.lastModifiedTime)
        },
        owners = cg.owners,
        threads = cg.threads)
    }
  }

  def getConsumerSkeletons: Seq[ConsumerSkeletonJs] = {
    KafkaMicroConsumer.kafkaUtil.getConsumerGroupIds map (ConsumerSkeletonJs(_))
  }

  def getConsumerOffsets(groupId: String): Seq[ConsumerOffsetUpdateJs] = {
    KafkaMicroConsumer.kafkaUtil.getConsumerOffsets(groupId) map { o =>
      ConsumerOffsetUpdateJs(
        consumerId = groupId,
        topic = o.topic,
        partition = o.partition,
        offset = o.offset,
        lastModifiedTime = o.lastModifiedTime)
    }
  }

  /**
    * Returns the Kafka-native consumer groups
    * @return the Kafka-native consumer groups
    */
  private def getConsumerGroupNative(groupId: String, autoOffsetReset: String = "earliest"): Option[ConsumerGroupJs] = {
    KafkaMicroConsumer.getConsumerGroupsFromKafka(Seq(groupId), autoOffsetReset) map (_.toJson(getLimitOffsets)) headOption
  }

  /**
    * Returns the Kafka-native consumer groups
    * @return the Kafka-native consumer groups
    */
  private def getConsumerGroupsNativeOffsets(autoOffsetReset: String = "earliest")(implicit ec: ExecutionContext): Seq[ConsumerOffset] = {
    KafkaMicroConsumer.getConsumerGroupOffsetsFromKafka(config.getConsumerGroupList, autoOffsetReset)
  }

  /**
    * Returns the Kafka Spout consumers (Partition Manager)
    * @return the Kafka Spout consumers
    */
  private def getConsumerGroupsPM(implicit ec: ExecutionContext): Seq[ConsumerDetailJs] = {
    KafkaMicroConsumer.getConsumersForStorm() map { c =>
      val topicOffset = Try(getLastOffset(c.topic, c.partition)) getOrElse None
      val messagesLeft = topicOffset map (offset => Math.max(0L, offset - c.offset))
      c.toJson(topicOffset, messagesLeft)
    }
  }

  /**
    * Returns a decoder by topic
    * @return the collection of decoders
    */
  def getDecoderByTopic(topic: String): DecoderJs = toDecoderJs(topic, config.getDecodersByTopic(topic))

  /**
    * Returns a decoder by topic and schema name
    * @return the option of a decoder
    */
  def getDecoderSchemaByName(topic: String, schemaName: String): String = {
    val decoders = config.getDecoders.filter(_.topic == topic)
    decoders.filter(_.name == schemaName)
      .map(_.decoder.schemaString)
      .headOption.orDie(s"No decoder named '$schemaName' was found for topic $topic")
  }

  /**
    * Returns all available decoders
    * @return the collection of decoders
    */
  def getDecoders: Seq[DecoderJs] = {
    (config.getDecoders.groupBy(_.topic) map { case (topic, myDecoders) =>
      toDecoderJs(topic, myDecoders.sortBy(-_.lastModified))
    }).toSeq
  }

  private def toDecoderJs(topic: String, decoders: Seq[TxDecoder]) = {
    val schemas = decoders map { d =>
      d.decoder match {
        case TxSuccessSchema(name, _, schemaString) => SchemaJs(topic, name, JsonHelper.renderJson(schemaString, pretty = true), error = None)
        case TxFailedSchema(name, e, schemaString) => SchemaJs(topic, name, schemaString, error = Some(e.getMessage))
      }
    }
    DecoderJs(topic, schemas)
  }

  /**
    * Returns the last offset for a given topic
    */
  def getLastOffset(topic: String, partition: Int): Option[Long] = {
    new KafkaMicroConsumer(TopicAndPartition(topic, partition), brokers) use (_.getLastOffset)
  }

  /**
    * Returns the first and last offsets for a given topic
    */
  def getLimitOffsets(topic: String, partition: Int): Option[(Long, Long)] = {
    new KafkaMicroConsumer(TopicAndPartition(topic, partition), brokers) use { subs =>
      for {
        first <- subs.getFirstOffset
        last <- subs.getLastOffset
      } yield (first, last)
    }
  }

  /**
    * Retrieves the message data for given topic, partition and offset
    * @param topic     the given topic
    * @param partition the given partition
    * @param offset    the given offset
    * @return the JSON representation of the message
    */
  def getMessageData(topic: String, partition: Int, offset: Long, decode: Boolean): MessageJs = {
    new KafkaMicroConsumer(TopicAndPartition(topic, partition), brokers) use { consumer =>
      getDefinedOffset(consumer, offset) flatMap { theOffset =>
        decodeMessageData(topic, consumer.fetch(theOffset)(fetchSize = DEFAULT_FETCH_SIZE).headOption, decode)
      } getOrElse {
        MessageJs(`type` = "error", message = "Offset is undefined")
      }
    }
  }

  /**
    * Retrieves the message key for given topic, partition and offset
    * @param topic     the given topic
    * @param partition the given partition
    * @param offset    the given offset
    * @return the JSON representation of the message key
    */
  def getMessageKey(topic: String, partition: Int, offset: Long, decode: Boolean): MessageJs = {
    new KafkaMicroConsumer(TopicAndPartition(topic, partition), brokers) use { consumer =>
      getDefinedOffset(consumer, offset) flatMap { o =>
        consumer.fetch(o)(fetchSize = DEFAULT_FETCH_SIZE).headOption map (md => toMessage(md.key, decode))
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
    * @param topic     the given Kafka topic
    * @param message_? an option of a [[MessageData]]
    * @return an option of a decoded message
    */
  private def decodeMessageData(topic: String, message_? : Option[MessageData], decode: Boolean): Option[MessageJs] = {
    if (!decode) message_?.map(toMessage(_, decode))
    else {
      message_? flatMap { md =>
        config.getDecodersByTopic(topic).foldLeft[Option[MessageJs]](None) { (result, d) =>
          result ?? attemptJsonDecode(md, d)
        } ?? message_?.map(toMessage(_, decode))
      }
    }
  }

  /**
    * Attempts to decode the given message with the given decoder
    * @param md        the given [[MessageData]]
    * @param txDecoder the given [[TxDecoder]]
    * @return an option of a decoded message
    */
  private def attemptJsonDecode(md: MessageData, txDecoder: TxDecoder): Option[MessageJs] = {
    txDecoder.decoder match {
      case TxSuccessSchema(name, decoder, _) =>
        decoder match {
          case jtc: JsonTransCoding =>
            jtc.decodeAsJson(md.message) match {
              case Success(jValue) =>
                Option(MessageJs(`type` = "json", partition = md.partition, offset = md.offset, payload = Json.parse(JsonHelper.renderJson(jValue, pretty = false))))
              case Failure(e) =>
                Logger.warn(s"$name: Decoding failure", e)
                None
            }
          case _ => None
        }
      case _ => None
    }
  }

  private def toMessage(message: Any, decode: Boolean): MessageJs = message match {
    case opt: Option[Any] => toMessage(opt.orNull, decode)
    case bytes: Array[Byte] if bytes.isPrintable =>
      MessageJs(`type` = "ascii", payload = JsString(new String(bytes)))
    case bytes: Array[Byte] =>
      MessageJs(`type` = "bytes", payload = toByteArray(bytes))
    case md: MessageData if decode && JsonHelper.isJson(new String(md.message)) =>
      MessageJs(`type` = "json", partition = md.partition, offset = md.offset, payload = Json.parse(new String(md.message)))
    case md: MessageData if md.message.isPrintable =>
      MessageJs(`type` = "ascii", partition = md.partition, offset = md.offset, payload = JsString(new String(md.message)))
    case md: MessageData =>
      MessageJs(`type` = "bytes", partition = md.partition, offset = md.offset, payload = toByteArray(md.message))
    case value =>
      MessageJs(`type` = "json", payload = Json.parse(value.toString))
  }

  /**
    * Retrieves the list of available queries for the any topic
    * @return the list of available queries
    */
  def getQueries: Seq[QueryDetailsJs] = config.getQueries.map(_.map(_.asJson)) getOrElse Nil

  /**
    * Retrieves the list of available queries for the given topic
    * @param topic the given topic (e.g. "shocktrade.quotes.avro")
    * @return the list of available queries
    */
  def getQueriesByTopic(topic: String): Seq[QueryDetailsJs] = {
    config.getQueriesByTopic(topic).map(_.map(_.asJson)) getOrElse Nil
  }

  /**
    * Retrieves the list of Kafka replicas for a given topic
    */
  def getReplicas(topic: String): Iterable[ReplicaJs] = {
    KafkaMicroConsumer.getReplicas(topic, brokers)
      .map(r => (r.partition, ReplicaHostJs(s"${r.host}:${r.port}", r.inSync)))
      .groupBy(_._1)
      .map { case (partition, replicas) => ReplicaJs(partition, replicas.map(_._2)) }
  }

  def getTopics: Seq[TopicDetailsJs] = KafkaMicroConsumer.getTopicList(brokers).map(_.asJson)

  def getTopicDeltas: List[TopicDeltaJs] = {
    val firstCall = cachedDeltaTopics.isEmpty

    val rawDeltas = for {
      state <- KafkaMicroConsumer.kafkaUtil.getBrokerTopics
      (startOffset, endOffset) <- getLimitOffsets(state.topic, state.partition)
    } yield TopicDeltaJs(state.topic, state.partition, startOffset, endOffset, messages = Math.max(endOffset - startOffset, 0L), 0L)

    // compute the total message count and add it to each delta
    val deltas = rawDeltas.groupBy(_.topic) flatMap { case (_, partitions) =>
      val totalMessages = partitions.map(_.messages).sum
      partitions.map(_.copy(totalMessages = totalMessages))
    }

    // filter out the ones that haven't change
    val filteredDeltas = deltas.filterNot(d => cachedDeltaTopics.get(d.topic -> d.partition).contains(d))

    // add the filtered deltas to the cache
    filteredDeltas.foreach(d => cachedDeltaTopics.put(d.topic -> d.partition, d))

    // if the cache was previously empty, send nothing
    if(firstCall) Nil else filteredDeltas.toList
  }

  def getTopicSummaries: Seq[TopicSummaryJs] = {
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

  def getTopicByName(topic: String): Option[TopicDetailsJs] = {
    KafkaMicroConsumer.getTopicList(brokers).find(_.topic == topic).map(_.asJson)
  }

  def getTopicDetailsByName(topic: String): Seq[TopicDetailsJs] = {
    KafkaMicroConsumer.getTopicPartitions(topic) map { partition =>
      new KafkaMicroConsumer(TopicAndPartition(topic, partition), brokers) use { consumer =>
        val startOffset = consumer.getFirstOffset
        val endOffset = consumer.getLastOffset
        val messages = for {start <- startOffset; end <- endOffset} yield Math.max(0L, end - start)
        TopicDetailsJs(topic, partition, startOffset, endOffset, messages)
      }
    }
  }

  def getZkData(path: String, format: String): Option[FormattedDataJs] = {
    val decoder = codecs.get(format).orDie(s"No decoder of type '$format' was found")
    zk.read(path) map decoder.decode match {
      case Some(Success(bytes: Array[Byte])) => Option(FormattedDataJs(`type` = BINARY, toByteArray(bytes)))
      case Some(Success(js: net.liftweb.json.JValue)) => Option(FormattedDataJs(`type` = JSON, Json.parse(net.liftweb.json.compactRender(js))))
      case Some(Success(s: String)) => Option(FormattedDataJs(`type` = PLAIN_TEXT, JsString(s)))
      case Some(Success(v)) => Option(FormattedDataJs(`type` = format, JsString(v.toString))) // TODO detect type
      case _ => None
    }
  }

  def getZkInfo(path: String): ZkItemInfoJs = {
    val creationTime = zk.getCreationTime(path)
    val lastModified = zk.getModificationTime(path)
    val data_? = zk.read(path)
    val size_? = data_? map (_.length)
    val formattedData_? = data_? map (bytes => FormattedDataJs(`type` = BINARY, toByteArray(bytes)))
    ZkItemInfoJs(path, creationTime, lastModified, size_?, formattedData_?)
  }

  def getZkPath(parentPath: String): Seq[ZkItemJs] = {
    zk.getChildren(parentPath) map { name =>
      ZkItemJs(name, path = if (parentPath == "/") s"/$name" else s"$parentPath/$name")
    }
  }

  def publishMessage(topic: String, payload: String)(implicit ec: ExecutionContext): Future[RecordMetadata] = {
    // deserialize the JSON
    val blob = JsonHelper.transformTo[MessageBlobJs](payload)

    // publish the message
    val publisher = cachedPublisher.getOrElseUpdate((), createPublisher(brokers))
    Future(blocking(publisher.publish(
      topic,
      key = blob.key map (key => toBinary(topic, key, blob.keyFormat)) getOrElse toDefaultBinary(blob.keyFormat),
      message = toBinary(topic, blob.message, blob.messageFormat)).get()))
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
    * @param value  the given value
    * @param format the specified format
    * @return the binary result
    */
  private def toBinary(topic: String, value: String, format: String): Array[Byte] = {
    format match {
      case "ASCII" => value.getBytes(config.encoding)
      case "Avro" => toAvroBinary(topic, value).orDie(s"No suitable decoder found for topic $topic")
      case "JSON" => JsonHelper.renderJson(value, pretty = false).getBytes(config.encoding)
      case "Hex-Notation" => ParsingHelper.parseDottedHex(value)
      case "EPOC" => ByteBufferUtils.longToBytes(value.toLong)
      case "UUID" => ByteBufferUtils.uuidToBytes(UUID.fromString(value))
      case _ =>
        throw new IllegalArgumentException(s"Invalid format type '$format'")
    }
  }

  /**
    * Transcodes the given JSON document into an Avro-compatible byte array
    * @param topic   the given topic (e.g. "shocktrade.keystats.avro")
    * @param jsonDoc the given JSON document
    * @return an option of an Avro-compatible byte array
    */
  private def toAvroBinary(topic: String, jsonDoc: String): Option[Array[Byte]] = {
    config.getDecodersByTopic(topic).foldLeft[Option[Array[Byte]]](None) { (result, txDecoder) =>
      result ?? txDecoder.decoder.successOption.flatMap {
        case TxSuccessSchema(name, decoder, _) =>
          decoder match {
            case av@AvroDecoder(label, schema) =>
              Try(AvroConversion.transcodeJsonToAvroBytes(jsonDoc, schema, config.encoding)) match {
                case Success(bytes) =>
                  Logger.info(s"$label: produced ${bytes.length} bytes")
                  Option(bytes)
                case Failure(e) =>
                  Logger.warn(s"$label: failed with '${e.getMessage}'")
                  None
              }
            case _ =>
              Logger.warn(s"Decoder $name is not Avro compatible")
              None
          }
        case _ => None
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

  private def createParentDirectory(file: File) {
    val parentDirectory = file.getParentFile
    if (!parentDirectory.exists) {
      Logger.info(s"Creating directory '${parentDirectory.getAbsolutePath}'...")
      if (!parentDirectory.mkdirs()) {
        Logger.warn(s"Directory '${parentDirectory.getAbsolutePath}' could not be created")
      }
    }
  }

  /**
    * Transforms the given JSON query results into comma separated values
    * @param queryResults the given query results (as a JSON string)
    * @return a collection of comma separated values
    */
  def transformResultsToCSV(queryResults: String)(implicit ec: ExecutionContext): Future[Option[List[String]]] = {
    def toCSV(values: List[String]): String = values.map(s => s""""$s"""").mkString(",")

    Future {
      val js = JsonHelper.transform(queryResults)
      for {
        topic <- (js \ "topic").extractOpt[String]
        labels <- (js \ "labels").extractOpt[List[String]]
        values <- (js \ "values").extractOpt[List[Map[String, String]]]
        rows = values map (m => labels map (m.getOrElse(_, "")))
      } yield toCSV(labels) :: (rows map toCSV)
    }
  }

  private def brokers: Seq[Broker] = {
    cachedBrokers.getOrElseUpdate((), KafkaMicroConsumer.getBrokerList map (b => Broker(b.host, b.port)))
  }

  private def rt(implicit ec: ExecutionContext) = {
    cachedRuntime.getOrElseUpdate((), {
      TxRuntimeContext(config, new MessageSourceFactory()
        .addReader("topic", new KafkaMessageReader(brokers))
        .addWriter("topic", new KafkaMessageWriter(brokers)))
    })
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
object KafkaPlayRestFacade {
  // Formatting Constants
  val AUTO = "auto"
  val BINARY = "binary"
  val JSON = "json"
  val PLAIN_TEXT = "plain-text"

  private val codecs = Map[String, MessageDecoder[_ <: AnyRef]](
    AUTO -> AutoDecoder,
    BINARY -> LoopBackCodec,
    PLAIN_TEXT -> PlainTextCodec,
    JSON -> JsonMessageDecoder)

  class KafkaMessageReader(brokers: Seq[Broker])(implicit zk: ZKProxy) extends MessageReader {
    override def getInputSource(url: String): Option[MessageInputSource] = {
      MessageSourceFactory.parseSourceURL(url) map { case (_, topic) =>
        new KafkaTopicMessageInputSource(brokers, topic)
      }
    }
  }

  class KafkaMessageWriter(brokers: Seq[Broker])(implicit zk: ZKProxy) extends MessageWriter {
    override def getOutputSource(url: String): Option[MessageOutputSource] = {
      MessageSourceFactory.parseSourceURL(url) map { case (_, topic) =>
        new KafkaTopicMessageOutputSource(brokers, topic)
      }
    }
  }

  implicit class ConsumerGroupJsExtensions(val cg: ConsumerGroup) extends AnyVal {

    def toJson(getLimitOffsets: (String, Int) => Option[(Long, Long)]): ConsumerGroupJs = {
      ConsumerGroupJs(
        consumerId = cg.consumerId,
        offsets = cg.offsets.map { o =>
          val limits = getLimitOffsets(o.topic, o.partition)
          ConsumerOffsetJs(
            groupId = o.groupId,
            topic = o.topic,
            partition = o.partition,
            offset = o.offset,
            topicStartOffset = limits.map(_._1),
            topicEndOffset = limits.map(_._2),
            messages = limits map { case (start, end) => Math.max(end - start, 0L) },
            lastModifiedTime = o.lastModifiedTime)
        },
        owners = cg.owners,
        threads = cg.threads)
    }
  }

}
