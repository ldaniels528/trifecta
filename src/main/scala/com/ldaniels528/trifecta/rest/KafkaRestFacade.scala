package com.ldaniels528.trifecta.rest

import java.io.{File, FileOutputStream}
import java.util.concurrent.Executors

import com.ldaniels528.trifecta.TxConfig.TxDecoder
import com.ldaniels528.trifecta.io.json.{JsonDecoder, JsonHelper}
import com.ldaniels528.trifecta.io.kafka.KafkaMicroConsumer.{LeaderAndReplicas, MessageData}
import com.ldaniels528.trifecta.io.kafka.{Broker, KafkaMicroConsumer, KafkaPublisher}
import com.ldaniels528.trifecta.io.zookeeper.ZKProxy
import com.ldaniels528.trifecta.messages.MessageCodecs.{LoopBackCodec, PlainTextCodec}
import com.ldaniels528.trifecta.messages.logic.Condition
import com.ldaniels528.trifecta.messages.logic.Expressions.{AND, Expression, OR}
import com.ldaniels528.trifecta.messages.query.parser.{KafkaQueryTokenizer, KafkaQueryParser}
import com.ldaniels528.trifecta.messages.query.{KQLResult, KQLSelection}
import com.ldaniels528.trifecta.messages.{CompositeTxDecoder, MessageDecoder}
import com.ldaniels528.trifecta.rest.KafkaRestFacade._
import com.ldaniels528.trifecta.rest.TxWebConfig._
import com.ldaniels528.trifecta.util.OptionHelper._
import com.ldaniels528.trifecta.util.ResourceHelper._
import com.ldaniels528.trifecta.util.StringHelper._
import com.ldaniels528.trifecta.{TxConfig, TxRuntimeContext}
import kafka.common.TopicAndPartition
import net.liftweb.json.{Extraction, JValue}
import org.slf4j.LoggerFactory

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext}
import scala.util.{Failure, Success, Try}

/**
 * Kafka REST Facade
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
case class KafkaRestFacade(config: TxConfig, zk: ZKProxy, correlationId: Int = 0) {
  private implicit val formats = net.liftweb.json.DefaultFormats
  private implicit val zkProxy: ZKProxy = zk
  private val logger = LoggerFactory.getLogger(getClass)
  private var topicCache: Map[(String, Int), TopicDelta] = Map.empty
  private var consumerCache: Map[ConsumerDeltaKey, ConsumerJs] = Map.empty
  private var publisher_? : Option[KafkaPublisher] = None

  // define the custom thread pool
  private implicit val ec = new ExecutionContext {
    private val threadPool = Executors.newFixedThreadPool(50)

    def execute(runnable: Runnable) = {
      threadPool.submit(runnable)
      ()
    }

    def reportFailure(t: Throwable) = logger.error("Error from thread pool", t)
  }

  private val rt = TxRuntimeContext(config)

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

  private val brokers: Seq[Broker] = KafkaMicroConsumer.getBrokerList(zk) map (b => Broker(b.host, b.port))

  def executeQuery(jsonString: String): JValue = {
    Try {
      val query = JsonHelper.transform[QueryJs](jsonString)
      val asyncIO = rt.executeQuery(compileQuery(query.queryString))
      Await.result(asyncIO.task, 30.minutes)
    } match {
      case Success(result: KQLResult) => Extraction.decompose(result)
      case Success(_) => Extraction.decompose(ErrorJs("Query string expected"))
      case Failure(e) =>
        logger.error("Query error", e)
        Extraction.decompose(ErrorJs(e.getMessage))
    }
  }

  private def compileQuery(queryString: String): KQLSelection = {
    val query = KafkaQueryParser(queryString)
    if (query.source.decoderURL != "default") query
    else {
      val topic = query.source.deviceURL.split("[:]").last
      query.copy(source = query.source.copy(decoderURL = topic))
    }
  }

  def findOne(topic: String, criteria: String): JValue = {
    Try {
      logger.info(s"topic = '$topic', criteria = '$criteria")
      val decoder_? = rt.lookupDecoderByName(topic)
      val conditions = parseCondition(criteria, decoder_?)
      (decoder_?, conditions)
    } match {
      case Success((decoder_?, conditions)) =>
        val outcome = KafkaMicroConsumer.findOne(topic, brokers, forward = true, conditions) map (
          _ map { case (partition, md) => (partition, md.offset, decoder_?.map(_.decode(md.message)))
          })
        Await.result(outcome, 30.minutes) match {
          case Some((partition, offset, Some(Success(message)))) =>
            Extraction.decompose(MessageJs(`type` = "json", payload = message.toString, topic = Option(topic), partition = Some(partition), offset = Some(offset)))
          case other =>
            logger.warn(s"Failed to retrieve a message: result => $other")
            Extraction.decompose(ErrorJs("Failed to retrieve a message"))
        }
      case Failure(e) =>
        Extraction.decompose(ErrorJs(e.getMessage))
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
  def getBrokers: JValue = Extraction.decompose(brokers)

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
  def getConsumers: JValue = Extraction.decompose(getConsumerGroupsNative ++ getConsumerGroupsPM)

  /**
   * Returns all consumers for all topics
   * @return a list of consumers
   */
  def getConsumerSet: JValue = {
    val consumers = getConsumerGroupsNative ++ (if (config.consumersPartitionManager) getConsumerGroupsPM else Nil)

    Extraction.decompose(consumers.groupBy(_.topic) map { case (topic, consumersA) =>
      val results = (consumersA.groupBy(_.consumerId) map { case (consumerId, consumersB) =>
        ConsumerConsumerJs(consumerId, consumersB)
      }).toSeq
      ConsumerTopicJs(topic, results)
    })
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
  def getDecoderByTopic(topic: String): JValue = {
    Extraction.decompose(toDecoderJs(topic, config.getDecoders filter (_.topic == topic)))
  }

  /**
   * Returns all available decoders
   * @return the collection of decoders
   */
  def getDecoders: JValue = {
    val results = (config.getDecoders.groupBy(_.topic) map { case (topic, myDecoders) =>
      toDecoderJs(topic, myDecoders)
    }).toSeq sortBy (_.topic)

    // transform the results to JSON
    Extraction.decompose(results)
  }

  private def toDecoderJs(topic: String, decoders: Seq[TxDecoder]): DecoderJs = {
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
  def getLeaderAndReplicas(topic: String): JValue = {
    Extraction.decompose {
      KafkaMicroConsumer.getLeaderAndReplicas(topic, brokers) flatMap {
        case LeaderAndReplicas(partition, leader, replicas) =>
          replicas map (LeaderAndReplicasJs(partition, leader, _))
      } sortBy (_.partition)
    }
  }

  /**
   * Retrieves the message data for given topic, partition and offset
   * @param topic the given topic
   * @param partition the given partition
   * @param offset the given offset
   * @return the JSON representation of the message
   */
  def getMessageData(topic: String, partition: Int, offset: Long): JValue = {
    Extraction.decompose {
      Try {
        new KafkaMicroConsumer(TopicAndPartition(topic, partition), brokers) use { cons =>
          val newOffset = for {
            firstOffset <- cons.getFirstOffset
            lastOffset <- cons.getLastOffset
            adjOffset = offset match {
              case o if o < firstOffset => firstOffset
              case o if o > lastOffset => lastOffset
              case o => o
            }
          } yield adjOffset

          newOffset.map { o =>
            decodeMessageData(topic, cons.fetch(o)(fetchSize = 65536).headOption)
          } getOrElse ErrorJs("Offset is undefined")
        }
      } match {
        case Success(message) => message
        case Failure(e) => ErrorJs(e.getMessage)
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
  def getMessageKey(topic: String, partition: Int, offset: Long): JValue = {
    Extraction.decompose {
      Try {
        new KafkaMicroConsumer(TopicAndPartition(topic, partition), brokers) use { cons =>
          val newOffset = for {
            firstOffset <- cons.getFirstOffset
            lastOffset <- cons.getLastOffset
            adjOffset = offset match {
              case o if o < firstOffset => firstOffset
              case o if o > lastOffset => lastOffset
              case o => o
            }
          } yield adjOffset

          newOffset.map { o =>
            cons.fetch(o)(fetchSize = 65536).headOption map (md => toMessage(md.key))
          } getOrElse ErrorJs("Offset is undefined")
        }
      } match {
        case Success(message) => message
        case Failure(e) => ErrorJs(e.getMessage)
      }
    }
  }

  /**
   * Sequentially tests each decoder for the given topic until one is found that will decode the given message
   * @param topic the given Kafka topic
   * @param message_? an option of a [[MessageData]]
   * @return an option of a decoded message
   */
  private def decodeMessageData(topic: String, message_? : Option[MessageData]) = {
    val decoders = config.getDecoders.filter(_.topic == topic).sortBy(-_.lastModified)
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
    case value => MessageJs(`type` = "json", payload = JsonHelper.makePretty(String.valueOf(value)))
  }

  def getQueries: JValue = Extraction.decompose(config.getQueries)

  /**
   * Retrieves the list of Kafka replicas for a given topic
   */
  def getReplicas(topic: String): JValue = {
    Extraction.decompose(KafkaMicroConsumer.getReplicas(topic, brokers) sortBy (_.partition))
  }

  /**
   * Returns a collection of topics that have changed since the last call
   * @return a collection of [[TopicDelta]] objects
   */
  def getTopicDeltas: Seq[TopicDelta] = {
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

  def getTopics: JValue = Extraction.decompose(KafkaMicroConsumer.getTopicList(brokers))

  def getTopicSummaries: JValue = {
    Extraction.decompose(KafkaMicroConsumer.getTopicList(brokers).groupBy(_.topic) map { case (topic, details) =>
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
    })
  }

  def getTopicByName(topic: String): Option[JValue] = {
    KafkaMicroConsumer.getTopicList(brokers).find(_.topic == topic).map(Extraction.decompose)
  }

  def getTopicDetailsByName(topic: String): JValue = {
    Extraction.decompose(KafkaMicroConsumer.getTopicPartitions(topic) map { partition =>
      new KafkaMicroConsumer(TopicAndPartition(topic, partition), brokers) use { consumer =>
        val startOffset = consumer.getFirstOffset
        val endOffset = consumer.getLastOffset
        val messages = for {start <- startOffset; end <- endOffset} yield Math.max(0L, end - start)
        TopicDetailsJs(topic, partition, startOffset, endOffset, messages)
      }
    })
  }

  def getZkData(path: String, format: String): JValue = {
    import net.liftweb.json._

    Extraction.decompose(Try {
      val decoder = codecs.get(format).orDie(s"No decoder of type '$format' was found")
      zk.read(path) map decoder.decode
    } match {
      case Success(data) => data match {
        case Some(Success(bytes: Array[Byte])) => FormattedData(`type` = BINARY, toByteArray(bytes))
        case Some(Success(js: JValue)) => FormattedData(`type` = JSON, compact(render(js)))
        case Some(Success(s: String)) => FormattedData(`type` = PLAIN_TEXT, s)
        case Some(Success(v)) => FormattedData(`type` = format, v)
        case _ => ()
      }
      case Failure(e) => ErrorJs(message = e.getMessage)
    })
  }

  def getZkInfo(path: String): JValue = {
    Extraction.decompose(Try {
      val creationTime = zk.getCreationTime(path)
      val lastModified = zk.getModificationTime(path)
      val data_? = zk.read(path)
      val size_? = data_? map (_.length)
      val formattedData_? = data_? map (bytes => FormattedData(`type` = BINARY, toByteArray(bytes)))
      ZkItemInfo(path, creationTime, lastModified, size_?, formattedData_?)
    } match {
      case Success(info) => info
      case Failure(e) => ErrorJs(message = e.getMessage)
    })
  }

  def getZkPath(parentPath: String): JValue = {
    Extraction.decompose(Try {
      zk.getChildren(parentPath) map { name =>
        ZkItem(name, path = if (parentPath == "/") s"/$name" else s"$parentPath/$name")
      }
    } match {
      case Success(items) => items
      case Failure(e) => ErrorJs(message = e.getMessage)
    })
  }

  def publishMessage(topic: String, jsonString: String): JValue = {
    // if the publisher has not been created ...
    if (publisher_?.isEmpty) publisher_? = Option {
      val publisher = KafkaPublisher(brokers)
      publisher.open()
      publisher
    }

    // deserialize the JSON
    val blob = JsonHelper.transform[MessageBlobJs](jsonString)

    // publish the message
    publisher_? foreach (_.publish(topic, toBinary(blob.key), toBinary(blob.message, blob.format)))

    Extraction.decompose(true)
  }

  private def toBinary(value: String, format: String = "binary"): Array[Byte] = {
    value.getBytes(config.encoding)
  }

  def saveQuery(jsonString: String): JValue = {
    Try {
      // transform the JSON string into a query
      val query = JsonHelper.transform[QueryJs](jsonString)

      val file = new File(TxWebConfig.queriesDirectory, s"${query.name}.bdql")
      // TODO add a check for new vs. replace?

      new FileOutputStream(file) use { fos =>
        fos.write(query.queryString.getBytes(config.encoding))
      }
    } match {
      case Success(_) => Extraction.decompose(ErrorJs(message = "Saved", `type` = "success"))
      case Failure(e) => Extraction.decompose(ErrorJs(message = e.getMessage, `type` = "error"))
    }
  }

  def saveSchema(jsonString: String): JValue = {
    Try {
      // transform the JSON string into a schema
      val schema = JsonHelper.transform[SchemaJs](jsonString)

      val decoderFile = new File(new File(TxConfig.decoderDirectory, schema.topic), schema.name)
      logger.info(s"decoderFile = ${decoderFile.getAbsolutePath}")
      // TODO add a check for new vs. replace?

      new FileOutputStream(decoderFile) use { fos =>
        fos.write(schema.schemaString.getBytes(config.encoding))
      }
    } match {
      case Success(_) => Extraction.decompose(ErrorJs(message = "Saved", `type` = "success"))
      case Failure(e) => Extraction.decompose(ErrorJs(message = e.getMessage, `type` = "error"))
    }
  }

  /**
   * Transforms the given JSON query results into comma separated values
   * @param queryResults the given query results (as a JSON string)
   * @return a collection of comma separated values
   */
  def transformResultsToCSV(queryResults: String): Try[Option[List[String]]] = {
    def toCSV(values: List[String]): String = values.map(s => s""""$s"""").mkString(",")
    Try {
      val js = JsonHelper.toJson(queryResults)
      for {
        topic <- (js \ "topic").extractOpt[String]
        labels <- (js \ "labels").extractOpt[List[String]]
        values <- (js \ "values").extractOpt[List[Map[String, String]]]
        rows = values map (m => labels map (m.getOrElse(_, "")))
        csv = rows map toCSV
      } yield toCSV(labels) :: csv
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

  case class MessageBlobJs(key: String, message: String, format: String)

  case class MessageJs(`type`: String, payload: Any, topic: Option[String] = None, partition: Option[Int] = None, offset: Option[Long] = None)

  case class QueryJs(name: String, queryString: String)

  case class TopicDetailsJs(topic: String, partition: Int, startOffset: Option[Long], endOffset: Option[Long], messages: Option[Long])

  case class TopicDelta(topic: String, partition: Int, startOffset: Long, endOffset: Long, messages: Long)

  case class TopicSummaryJs(topic: String, partitions: Seq[TopicPartitionJs], totalMessages: Long)

  case class TopicPartitionJs(partition: Int, startOffset: Option[Long], endOffset: Option[Long], messages: Option[Long], leader: Option[Broker], replicas: Seq[Broker])

  case class ZkItem(name: String, path: String)

  case class ZkItemInfo(path: String, creationTime: Option[Long], lastModified: Option[Long], size: Option[Int], data: Option[FormattedData])

}
