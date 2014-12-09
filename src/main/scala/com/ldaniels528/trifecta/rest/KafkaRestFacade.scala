package com.ldaniels528.trifecta.rest

import java.io.{File, FileOutputStream}
import java.util.concurrent.Executors

import com.ldaniels528.trifecta.command.parser.bdql.{BigDataQueryParser, BigDataQueryTokenizer}
import com.ldaniels528.trifecta.io.json.{JsonDecoder, JsonHelper}
import com.ldaniels528.trifecta.io.kafka.{Broker, KafkaMicroConsumer}
import com.ldaniels528.trifecta.io.zookeeper.ZKProxy
import com.ldaniels528.trifecta.messages.MessageCodecs.{LoopBackCodec, PlainTextCodec}
import com.ldaniels528.trifecta.messages.MessageDecoder
import com.ldaniels528.trifecta.messages.logic.Condition
import com.ldaniels528.trifecta.messages.logic.Expressions.{AND, Expression, OR}
import com.ldaniels528.trifecta.messages.query.{BigDataSelection, QueryResult}
import com.ldaniels528.trifecta.rest.EmbeddedWebServer._
import com.ldaniels528.trifecta.rest.KafkaRestFacade._
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

  // define the custom thread pool
  private implicit val ec = new ExecutionContext {
    private val threadPool = Executors.newFixedThreadPool(50)

    def execute(runnable: Runnable) = threadPool.submit(runnable)

    def reportFailure(t: Throwable) = logger.error("Error from thread pool", t)
  }

  private val rt = TxRuntimeContext(config)

  // load & register all decoders for their respective topics
  for {decoders <- config.getDecoders; decoder <- decoders} rt.registerDecoder(decoder.topic, decoder.decoder)

  private val brokers: Seq[Broker] = KafkaMicroConsumer.getBrokerList(zk) map (b => Broker(b.host, b.port))

  def executeQuery(queryString_? : Option[String]): JValue = {
    Try {
      queryString_? map { queryString =>
        val asyncIO = rt.executeQuery(compileQuery(queryString))
        Await.result(asyncIO.task, 30.minutes)
      }
    } match {
      case Success(Some(result: QueryResult)) => Extraction.decompose(result)
      case Failure(e) =>
        logger.error("Query error", e)
        Extraction.decompose(ErrorJs(e.getMessage))
    }
  }

  private def compileQuery(queryString: String): BigDataSelection = {
    val query = BigDataQueryParser(queryString)
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
        val outcome = KafkaMicroConsumer.findOne(topic, brokers, correlationId = 0, forward = true, conditions) map (
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
    import com.ldaniels528.trifecta.command.parser.bdql.BigDataQueryParser.deQuote
    import com.ldaniels528.trifecta.messages.logic.ConditionCompiler._

    val it = BigDataQueryTokenizer.parse(expression).iterator
    var criteria: Option[Expression] = None
    while (it.hasNext) {
      val args = it.take(criteria.size + 3).toList
      criteria = args match {
        case List("and", field, operator, value) => criteria.map(AND(_, compile(field, operator, deQuote(value))))
        case List("or", field, operator, value) => criteria.map(OR(_, compile(field, operator, deQuote(value))))
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
    val consumers = getConsumerGroupsNative ++ getConsumerGroupsPM

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
   * Returns the first offset for a given topic
   */
  def getFirstOffset(topic: String, partition: Int)(implicit zk: ZKProxy): Option[Long] = {
    new KafkaMicroConsumer(TopicAndPartition(topic, partition), brokers, correlationId) use (_.getFirstOffset)
  }

  /**
   * Returns the last offset for a given topic
   */
  def getLastOffset(topic: String, partition: Int)(implicit zk: ZKProxy): Option[Long] = {
    new KafkaMicroConsumer(TopicAndPartition(topic, partition), brokers, correlationId) use (_.getLastOffset)
  }

  def getMessage(topic: String, partition: Int, offset: Long, decoderURL: Option[String] = None): JValue = {
    Extraction.decompose {
      Try {
        val message_? = new KafkaMicroConsumer(TopicAndPartition(topic, partition), brokers) use (_.fetch(offset)(fetchSize = 65536).headOption)
        val decoder_? = rt.lookupDecoderByName(topic)
        val decodedMessage = for {decoder <- decoder_?; data <- message_?} yield decoder.decode(data.message)
        decodedMessage match {
          case Some(Success(typedMessage)) => toMessage(typedMessage)
          case _ => toMessage(message_?.map(_.message).orNull)
        }
      } match {
        case Success(message) => message
        case Failure(e) => ErrorJs(e.getMessage)
      }
    }
  }

  private def toMessage(message: Any): MessageJs = message match {
    case opt: Option[Any] => toMessage(opt.orNull)
    case bytes: Array[Byte] => MessageJs(`type` = "bytes", payload = toByteArray(bytes))
    case value => MessageJs(`type` = "json", payload = JsonHelper.makePretty(String.valueOf(value)))
  }

  def getQueries: JValue = Extraction.decompose(config.getQueries)

  /**
   * Returns a collection of topics that have changed since the last call
   * @return a collection of [[TopicDelta]] objects
   */
  def getTopicDeltas: Seq[TopicDelta] = {
    // retrieve all topic/partitions for analysis
    val topics = KafkaMicroConsumer.getTopicList(brokers, correlationId) flatMap { t =>
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

  def getTopics: JValue = Extraction.decompose(KafkaMicroConsumer.getTopicList(brokers, correlationId))

  def getTopicSummaries: JValue = {
    Extraction.decompose(KafkaMicroConsumer.getTopicList(brokers, correlationId).groupBy(_.topic) map { case (topic, details) =>
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
    KafkaMicroConsumer.getTopicList(brokers, correlationId).find(_.topic == topic).map(Extraction.decompose)
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
      val decoder = decoders.get(format).orDie(s"No decoder of type '$format' was found")
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

  def saveQuery(name: Option[String], queryString: Option[String]): JValue = {
    val outcome = for {
      myName <- name
      myQueryString <- queryString
    } yield {
      val file = new File(config.queriesDirectory, s"$myName.bdql")
      // TODO add a check for new vs. replace?

      Try(new FileOutputStream(file) use { fos =>
        fos.write(myQueryString.getBytes(config.encoding))
      })
    }

    outcome match {
      case Some(Success(_)) => Extraction.decompose(ErrorJs(message = "Saved", `type` = "success"))
      case Some(Failure(e)) => Extraction.decompose(ErrorJs(message = e.getMessage, `type` = "error"))
      case _ => Extraction.decompose(ErrorJs(message = "Unknown error", `type` = "error"))
    }
  }

  /**
   * Transforms the given JSON query results into comma separated values
   * @param queryResults_? the given query results
   * @return a collection of comma separated values
   */
  def transformResultsToCSV(queryResults_? : Option[String]): Try[Option[List[String]]] = {
    def toCSV(values: List[String]): String = values.map(s => s""""$s"""").mkString(",")
    Try {
      for {
        js <- queryResults_? map JsonHelper.toJson
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

  private val decoders = Map[String, MessageDecoder[_ <: AnyRef]](
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

  case class ErrorJs(message: String, `type`: String = "error")

  case class FormattedData(`type`: String, value: Any)

  case class MessageJs(`type`: String, payload: Any, topic: Option[String] = None, partition: Option[Int] = None, offset: Option[Long] = None)

  case class TopicDetailsJs(topic: String, partition: Int, startOffset: Option[Long], endOffset: Option[Long], messages: Option[Long])

  case class TopicDelta(topic: String, partition: Int, startOffset: Long, endOffset: Long, messages: Long)

  case class TopicSummaryJs(topic: String, partitions: Seq[TopicPartitionJs], totalMessages: Long)

  case class TopicPartitionJs(partition: Int, startOffset: Option[Long], endOffset: Option[Long], messages: Option[Long], leader: Option[Broker], replicas: Seq[Broker])

  case class ZkItem(name: String, path: String)

  case class ZkItemInfo(path: String, creationTime: Option[Long], lastModified: Option[Long], size: Option[Int], data: Option[FormattedData])

}
