package com.ldaniels528.trifecta.rest

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
import com.ldaniels528.trifecta.rest.KafkaRestFacade._
import com.ldaniels528.trifecta.util.OptionHelper._
import com.ldaniels528.trifecta.util.ResourceHelper._
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
  private val decoders = Map(BINARY -> LoopBackCodec, PLAIN_TEXT -> PlainTextCodec, JSON -> JsonDecoder)

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

  def executeQuery(queryString: String): JValue = {
    logger.info(s"queryString = '$queryString'")
    Try {
      val asyncIO = rt.executeQuery(compileQuery(queryString))
      Await.result(asyncIO.task, 30.minutes)
    } match {
      case Success(result: QueryResult) => Extraction.decompose(result)
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
    logger.info(s"topic = '$topic', criteria = '$criteria")
    val decoder_? = rt.lookupDecoderByName(topic)
    val outcome = KafkaMicroConsumer.findOne(topic, brokers, correlationId = 0, parseCondition(criteria, decoder_?)) map (
      _ map { case (partition, md) => (partition, md.offset, decoder_?.map(_.decode(md.message)))
      })
    Await.result(outcome, 30.minutes) match {
      case Some((partition, offset, Some(Success(message)))) =>
        Extraction.decompose(MessageJs(`type` = "json", payload = message.toString, topic = Option(topic), partition = Some(partition), offset = Some(offset)))
      case other =>
        logger.warn(s"Failed to retrieve a message: result => $other")
        Extraction.decompose(())
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
      ConsumerJs(c.consumerId, c.topic, c.partition, c.offset, topicOffset, c.lastModified, delta)
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
      ConsumerJs(c.topologyName, c.topic, c.partition, c.offset, topicOffset, c.lastModified, delta)
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
    val message_? = new KafkaMicroConsumer(TopicAndPartition(topic, partition), brokers) use (_.fetch(offset, fetchSize = 65535).headOption)
    val decoder_? = rt.lookupDecoderByName(topic)
    val decodedMessage = for {decoder <- decoder_?; data <- message_?} yield decoder.decode(data.message)

    val message: MessageJs = decodedMessage match {
      case Some(Success(typedMessage)) => toMessage(typedMessage)
      case _ => toMessage(message_?.map(_.message).orNull)
    }
    Extraction.decompose(message)
  }

  private def toMessage(message: Any): MessageJs = message match {
    case opt: Option[Any] => toMessage(opt.orNull)
    case bytes: Array[Byte] => MessageJs(`type` = "bytes", payload = toByteArray(bytes))
    case value => MessageJs(`type` = "json", payload = JsonHelper.makePretty(String.valueOf(value)))
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
        case Some(Success(js: JValue)) => FormattedData(`type` = JSON, compact(render(js)))
        case Some(Success(bytes: Array[Byte])) => FormattedData(`type` = BINARY, toByteArray(bytes))
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
      val data = zk.read(path) map(bytes => FormattedData(`type` = BINARY, toByteArray(bytes)))
      ZkItemInfo(path, creationTime, lastModified, data)
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
  val BINARY = "binary"
  val PLAIN_TEXT = "plain-text"
  val JSON = "json"

  case class ConsumerJs(consumerId: String, topic: String, partition: Int, offset: Long, topicOffset: Option[Long], lastModified: Option[Long], messagesLeft: Option[Long])

  case class ConsumerTopicJs(topic: String, consumers: Seq[ConsumerConsumerJs])

  case class ConsumerConsumerJs(consumerId: String, details: Seq[ConsumerJs])

  case class ErrorJs(message: String, `type`: String = "error")

  case class FormattedData(`type`: String, value: AnyRef)

  case class MessageJs(`type`: String, payload: Any, topic: Option[String] = None, partition: Option[Int] = None, offset: Option[Long] = None)

  case class TopicDetailsJs(topic: String, partition: Int, startOffset: Option[Long], endOffset: Option[Long], messages: Option[Long])

  case class TopicSummaryJs(topic: String, partitions: Seq[TopicPartitionJs], totalMessages: Long)

  case class TopicPartitionJs(partition: Int, startOffset: Option[Long], endOffset: Option[Long], messages: Option[Long], leader: Option[Broker], replicas: Seq[Broker])

  case class ZkItem(name: String, path: String)

  case class ZkItemInfo(path: String, creationTime: Option[Long], lastModified: Option[Long], data: Option[FormattedData])

}
