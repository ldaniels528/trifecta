package com.ldaniels528.verify.modules.kafka

import java.io.{File, FileOutputStream, PrintStream}
import java.nio.ByteBuffer._
import java.text.SimpleDateFormat
import java.util.Date

import _root_.kafka.consumer.ConsumerTimeoutException
import com.ldaniels528.verify.VxRuntimeContext
import com.ldaniels528.verify.io.EndPoint
import com.ldaniels528.verify.modules.CommandParser.UnixLikeArgs
import com.ldaniels528.verify.modules._
import com.ldaniels528.verify.modules.avro.AvroConditions._
import com.ldaniels528.verify.modules.kafka.KafkaModule._
import com.ldaniels528.verify.support.avro.{AvroDecoder, AvroReading}
import com.ldaniels528.verify.support.kafka.KafkaSubscriber.{BrokerDetails, MessageData}
import com.ldaniels528.verify.support.kafka.{Condition, _}
import com.ldaniels528.verify.util.BinaryMessaging
import com.ldaniels528.verify.util.VxUtils._
import com.ldaniels528.verify.vscript.VScriptRuntime.ConstantValue
import com.ldaniels528.verify.vscript.{Scope, Variable}

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits._
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.{Failure, Success}

/**
 * Kafka Module
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class KafkaModule(rt: VxRuntimeContext) extends Module with BinaryMessaging with AvroReading {
  private implicit val out: PrintStream = rt.out
  private implicit val scope: Scope = rt.scope
  private implicit val rtc: VxRuntimeContext = rt

  // create the ZooKeeper proxy
  private implicit val zk = rt.zkProxy

  // get the list of brokers from zookeeper
  private val brokers: Seq[Broker] = KafkaSubscriber.getBrokerList(zk) map (b => Broker(b.host, b.port))

  // set the default correlation ID
  private val correlationId: Int = (Math.random * Int.MaxValue).toInt

  // incoming messages cache
  private var incomingMessageCache = Map[TopicSlice, Inbound]()
  private var lastInboundCheck: Long = _

  // define the offset for message cursor navigation commands
  private var cursor: Option[MessageCursor] = None

  def defaultFetchSize = scope.getValue[Int]("defaultFetchSize") getOrElse 65536

  def defaultFetchSize_=(sizeInBytes: Int) = scope.setValue("defaultFetchSize", Option(sizeInBytes))

  def parallelism = scope.getValue[Int]("parallelism") getOrElse 4

  def parallelism_=(parallelism: Int) = scope.setValue("parallelism", Option(parallelism))

  // the bound commands
  override def getCommands: Seq[Command] = Seq(
    Command(this, "kbrokers", getBrokers, UnixLikeParams(), help = "Returns a list of the brokers from ZooKeeper"),
    Command(this, "kcommit", commitOffset, UnixLikeParams(Seq("topic" -> false, "partition" -> false, "groupId" -> true, "offset" -> true), Seq("-m" -> "metadata")), help = "Commits the offset for a given topic and group"),
    Command(this, "kconsumers", getConsumers, SimpleParams(Seq.empty, Seq("topicPrefix")), help = "Returns a list of the consumers from ZooKeeper"),
    Command(this, "kcount", countMessages, SimpleParams(Seq("field", "operator", "value"), Seq.empty), help = "Counts the messages matching a given condition [references cursor]"),
    Command(this, "kcursor", showCursor, UnixLikeParams(), help = "Displays the current message cursor"),
    Command(this, "kexport", exportToFile, UnixLikeParams(Seq("topic" -> false, "groupId" -> true), Seq("-f" -> "outputFile")), help = "Writes the contents of a specific topic to a file", undocumented = true),
    Command(this, "kfetch", fetchOffsets, UnixLikeParams(Seq("topic" -> false, "partition" -> false, "groupId" -> true)), help = "Retrieves the offset for a given topic and group"),
    Command(this, "kfetchsize", fetchSizeGetOrSet, SimpleParams(Seq.empty, Seq("fetchSize")), help = "Retrieves or sets the default fetch size for all Kafka queries"),
    Command(this, "kfindone", findOneMessage, SimpleParams(Seq("field", "operator", "value"), Seq.empty), "Returns the first message that corresponds to the given criteria [references cursor]"),
    Command(this, "kfirst", getFirstMessage, UnixLikeParams(Seq("topic" -> false, "partition" -> false), Seq("-a" -> "avroSchema", "-f" -> "outputFile")), help = "Returns the first message for a given topic"),
    Command(this, "kget", getMessage, UnixLikeParams(Seq("topic" -> false, "partition" -> false, "offset" -> true), Seq("-a" -> "avroSchema", "-f" -> "outputFile")), help = "Retrieves the message at the specified offset for a given topic partition"),
    Command(this, "kgetsize", getMessageSize, UnixLikeParams(Seq("topic" -> false, "partition" -> false, "offset" -> true), Seq("-s" -> "fetchSize")), help = "Retrieves the size of the message at the specified offset for a given topic partition"),
    Command(this, "kgetminmax", getMessageMinMaxSize, UnixLikeParams(Seq("topic" -> false, "partition" -> false, "startOffset" -> true, "endOffset" -> true), Seq("-s" -> "fetchSize")), help = "Retrieves the smallest and largest message sizes for a range of offsets for a given partition"),
    Command(this, "kimport", importMessages, UnixLikeParams(Seq("topic" -> false), Seq("-a" -> "avro", "-b" -> "binary", "-f" -> "inputFile", "-t" -> "fileType")), help = "Imports messages into a new/existing topic"),
    Command(this, "kinbound", inboundMessages, UnixLikeParams(Seq("topicPrefix" -> false)), help = "Retrieves a list of topics with new messages (since last query)"),
    Command(this, "klast", getLastMessage, UnixLikeParams(Seq("topic" -> false, "partition" -> false), Seq("-a" -> "avroSchema", "-f" -> "outputFile")), help = "Returns the last message for a given topic"),
    Command(this, "kls", getTopics, UnixLikeParams(Seq("topicPrefix" -> false)), help = "Lists all existing topics"),
    Command(this, "knext", getNextMessage, UnixLikeParams(flags = Seq("-a" -> "avroSchema", "-f" -> "outputFile")), help = "Attempts to retrieve the next message"),
    Command(this, "koffset", getOffset, UnixLikeParams(Seq("topic" -> false, "partition" -> false), Seq("-d" -> "time=YYYY-MM-DDTHH:MM:SS")), help = "Returns the offset at a specific instant-in-time for a given topic"),
    Command(this, "kprev", getPreviousMessage, UnixLikeParams(flags = Seq("-a" -> "avroSchema", "-f" -> "outputFile")), help = "Attempts to retrieve the message at the previous offset"),
    Command(this, "kpublish", publishMessage, SimpleParams(Seq("topic", "key"), Seq.empty), help = "Publishes a message to a topic"),
    Command(this, "kreplicas", getReplicas, SimpleParams(Seq.empty, Seq("prefix")), help = "Returns a list of replicas for specified topics"),
    Command(this, "kreset", resetConsumerGroup, UnixLikeParams(Seq("topic" -> false, "groupId" -> true)), help = "Sets a consumer group ID to zero for all partitions"),
    Command(this, "ksearch", findMessageByKey, SimpleParams(Seq.empty, Seq("topic", "groupId", "keyVariable")), help = "Scans a topic for a message with a given key (EXPERIMENTAL)", undocumented = true),
    Command(this, "kstats", getStatistics, UnixLikeParams(Seq("topic" -> false, "beginPartition" -> false, "endPartition" -> false)), help = "Returns the partition details for a given topic"))

  override def getVariables: Seq[Variable] = Seq(
    Variable("defaultFetchSize", ConstantValue(Option(65536)))
  )

  override def moduleName = "kafka"

  override def prompt: String = cursor map (c => s"${c.topic}/${c.partition}:${c.offset}") getOrElse "/"

  override def shutdown() = ()

  /**
   * "kcommit" - Commits the offset for a given topic and group ID
   * @example {{{ kcommit com.shocktrade.alerts 0 devc0 123678 }}}
   */
  def commitOffset(params: UnixLikeArgs): Option[Short] = {
    // get the arguments (topic, partition, groupId and offset)
    val (topic, partition, groupId, offset) = params.args match {
      case aGroupId :: anOffset :: Nil =>
        cursor map (c => (c.topic, c.partition, aGroupId, parseOffset(anOffset))) getOrElse dieNoCursor
      case aTopic :: aPartition :: aGroupId :: anOffset :: Nil =>
        (aTopic, parsePartition(aPartition), aGroupId, parseOffset(anOffset))
      case _ =>
        dieSyntax("kcommit")
    }

    // perform the action
    new KafkaSubscriber(TopicSlice(topic, partition), brokers, correlationId) use (
      _.commitOffsets(groupId, offset, params("-m") getOrElse "N/A"))
  }

  /**
   * "kcount" - Counts the messages matching a given condition [references cursor]
   * @example {{{ kcount frequency >= 1200 }}}
   */
  def countMessages(params: UnixLikeArgs): Future[Long] = {
    // get the topic and partition from the cursor
    val (topic, encoding) = cursor map (c => (c.topic, c.encoding)) getOrElse dieNoCursor

    // get the decoder
    val decoder = encoding match {
      case AvroMessageEncoding(schemaVarName) => getAvroDecoder(schemaVarName)
      case _ =>
        throw new IllegalArgumentException("Raw binary format is not supported")
    }

    // get the criteria
    val Seq(field, operator, value, _*) = params.args
    val conditions = operator match {
      case "==" => Seq(AvroEQ(decoder, field, value))
      case "!=" => Seq(AvroNotEQ(decoder, field, value))
      case ">" => Seq(AvroGreater(decoder, field, value))
      case "<" => Seq(AvroLesser(decoder, field, value))
      case ">=" => Seq(AvroGreaterOrEQ(decoder, field, value))
      case "<=" => Seq(AvroLesserOrEQ(decoder, field, value))
      case _ =>
        throw new IllegalArgumentException(s"Illegal operator near '$operator'")
    }

    // perform the count
    KafkaSubscriber.count(topic, brokers, correlationId, conditions: _*)
  }

  /**
   * "kexport" - Dumps the contents of a specific topic to a file
   * @example {{{  kexport com.shocktrade.quotes.csv lld3 -f quotes.bin }}}
   * @example {{{  kexport lld3 -f quotes.bin }}}
   */
  def exportToFile(params: UnixLikeArgs): Long = {
    import java.io.{DataOutputStream, FileOutputStream}

    // get the arguments (topic, groupId)
    val (topic, groupId) = params.args match {
      case aGroupId :: Nil => cursor map (c => (c.topic, aGroupId)) getOrElse dieNoCursor
      case aTopic :: aGroupId :: Nil => (aTopic, aGroupId)
      case _ => dieSyntax("kexport")
    }

    // get the output source
    val file = params("-f") getOrElse dieNoOutputSource
    // TODO add additional sources; including Cassandra, MySQL, Kafka topic, Kestrel queue

    // export the data to the file
    var count = 0L
    try {
      new DataOutputStream(new FileOutputStream(file)) use { fos =>
        KafkaStreamingConsumer(EndPoint(rt.remoteHost), groupId, "consumer.timeout.ms" -> 5000) use { consumer =>
          for (record <- consumer.iterate(topic, parallelism = 1)) {
            val message = record.message
            fos.writeInt(message.length)
            fos.write(message)
            count += 1
            if (count % 10000 == 0) {
              out.println(s"$count messages written so far...")
              fos.flush()
            }
          }
        }
      }
    } catch {
      case e: ConsumerTimeoutException =>
      case e: Throwable =>
        throw new IllegalStateException(e.getMessage, e)
    }
    count
  }

  /**
   * "kfetch" - Returns the offsets for a given topic and group ID
   * @example {{{ kfetch com.shocktrade.alerts 0 dev }}}
   * @example {{{ kfetch dev }}}
   */
  def fetchOffsets(params: UnixLikeArgs): Option[Long] = {
    // get the arguments (topic, partition, groupId)
    val (topic, partition, groupId) = params.args match {
      case aGroupId :: Nil =>
        cursor map (c => (c.topic, c.partition, aGroupId)) getOrElse dieNoCursor
      case aTopic :: aPartition :: aGroupId :: Nil =>
        (aTopic, parsePartition(aPartition), aGroupId)
      case _ =>
        dieSyntax("kfetch")
    }

    // perform the action
    new KafkaSubscriber(TopicSlice(topic, partition), brokers, correlationId) use (_.fetchOffsets(groupId))
  }

  /**
   * "kfetchsize" - Retrieves or sets the default fetch size for all Kafka queries
   * @param params the given command line arguments
   */
  def fetchSizeGetOrSet(params: UnixLikeArgs) = {
    params.args.headOption match {
      case Some(fetchSize) => defaultFetchSize = parseInt("fetchSize", fetchSize)
      case None => defaultFetchSize
    }
  }

  /**
   * "ksearch" - Scans a topic for a message with a given key
   * @example {{{ ksearch com.shocktrade.quotes.csv devGroup myKey }}}
   * @example {{{ ksearch devGroup myKey }}}
   */
  def findMessageByKey(params: UnixLikeArgs): Future[Option[MessageData]] = {
    // get the topic and partition arguments
    val (topic, groupId, keyVar) = params.args match {
      case aGroupId :: aKey :: Nil => cursor map (c => (c.topic, aGroupId, aKey)) getOrElse dieNoCursor
      case aTopic :: aGroupId :: aKey :: Nil => (aTopic, aGroupId, aKey)
      case _ => dieSyntax("ksearch")
    }

    // get the binary key
    val variable = scope.getVariable(keyVar) getOrElse die(s"Variable '$keyVar' not found")
    val key = variable.eval[Array[Byte]] getOrElse die(s"$keyVar is undefined")

    // get the consumer instance
    val consumer = KafkaStreamingConsumer(EndPoint(zk.remoteHost), groupId, "consumer.timeout.ms" -> 5000)

    // perform the search
    val result = consumer.scan(topic, parallelism = 4, BinaryKeyEqCondition(key)) map (_ map { msg =>
      val lastOffset: Long = getLastOffset(msg.topic, msg.partition) getOrElse -1L
      val nextOffset: Long = msg.offset + 1
      cursor = Option(MessageCursor(msg.topic, msg.partition, msg.offset, nextOffset, BinaryMessageEncoding))
      MessageData(msg.offset, nextOffset, lastOffset, msg.message)
    })

    // close the consumer once a response is available
    result.foreach(msg_? => consumer.close())
    result
  }

  /**
   * "kfindone" - Returns the first message that corresponds to the given criteria
   * @example {{{ kfindone frequency > 5000 }}}
   */
  def findOneMessage(params: UnixLikeArgs): Future[Option[Either[Option[MessageData], Option[Seq[AvroRecord]]]]] = {
    // get the topic and partition from the cursor
    val (topic, encoding) = cursor map (c => (c.topic, c.encoding)) getOrElse dieNoCursor

    // get the decoder
    val (schema, decoder) = encoding match {
      case AvroMessageEncoding(schemaVar) => (schemaVar, getAvroDecoder(schemaVar))
      case _ =>
        throw new IllegalArgumentException("Only Avro format is supported")
    }

    // get the criteria
    val Seq(field, operator, value, _*) = params.args
    val conditions = operator match {
      case "==" => Seq(AvroEQ(decoder, field, value))
      case "!=" => Seq(AvroNotEQ(decoder, field, value))
      case ">" => Seq(AvroGreater(decoder, field, value))
      case "<" => Seq(AvroLesser(decoder, field, value))
      case ">=" => Seq(AvroGreaterOrEQ(decoder, field, value))
      case "<=" => Seq(AvroLesserOrEQ(decoder, field, value))
      case _ =>
        throw new IllegalArgumentException(s"Illegal operator near '$operator'")
    }

    // perform the search
    val promise = KafkaSubscriber.findOne(topic, brokers, correlationId, conditions: _*)
    promise.map { optResult =>
      for {
        (partition, md) <- optResult
        encoding <- cursor map (_.encoding)
      } yield getMessage(topic, partition, md.offset, UnixLikeArgs(Nil, Map("-a" -> Option(schema))))
    }
  }

  /**
   * "kbrokers" - Retrieves the list of Kafka brokers
   */
  def getBrokers(args: UnixLikeArgs): Seq[BrokerDetails] = KafkaSubscriber.getBrokerList

  /**
   * "kconsumers" - Retrieves the list of Kafka consumers
   */
  def getConsumers(params: UnixLikeArgs): Seq[ConsumerDelta] = {
    // get the optional topic prefix
    val topicPrefix = params.args.headOption

    // retrieve the data
    val consumers = KafkaSubscriber.getConsumerList(topicPrefix).sortBy(c => (c.consumerId, c.topic, c.partition))
    consumers map { c =>
      val topicOffset = getLastOffset(c.topic, c.partition)
      val delta = topicOffset map (offset => Math.max(0, offset - c.offset))
      ConsumerDelta(c.consumerId, c.topic, c.partition, c.offset, topicOffset, delta)
    }
  }

  case class ConsumerDelta(consumerId: String, topic: String, partition: Int, offset: Long, topicOffset: Option[Long], messagesLeft: Option[Long])

  /**
   * "kcursor" - Displays the current message cursor
   * @example {{{ kcursor }}}
   * @example {{{ kcursor 5 }}}
   * @example {{{ kcursor shocktrade.quotes.csv 0 }}}
   */
  def showCursor(params: UnixLikeArgs): Seq[MessageCursor] = {
    cursor.map(c => Seq(c)) getOrElse Seq.empty
  }

  /**
   * "kfirst" - Returns the first message for a given topic
   * @example {{{ kfirst com.shocktrade.quotes.csv 0 }}}
   */
  def getFirstMessage(params: UnixLikeArgs) = {
    // get the arguments
    val (topic, partition) = getTopicAndPartition(params.args)

    // return the first record with the cursor's encoding
    getMessage(topic, partition, 0L, params)
  }

  /**
   * Returns the first offset for a given topic
   */
  def getFirstOffset(topic: String, partition: Int): Option[Long] = {
    new KafkaSubscriber(TopicSlice(topic, partition), brokers, correlationId) use (_.getFirstOffset)
  }

  /**
   * "klast" - Returns the last offset for a given topic
   */
  def getLastMessage(params: UnixLikeArgs) = {
    // get the arguments
    val (topic, partition) = getTopicAndPartition(params.args)

    // perform the action
    val lastOffset = new KafkaSubscriber(TopicSlice(topic, partition), brokers, correlationId) use (_.getLastOffset)
    lastOffset map (getMessage(topic, partition, _, params))
  }

  /**
   * Returns the last offset for a given topic
   */
  def getLastOffset(topic: String, partition: Int): Option[Long] = {
    new KafkaSubscriber(TopicSlice(topic, partition), brokers, correlationId) use (_.getLastOffset)
  }

  /**
   * "kget" - Returns the message for a given topic partition and offset
   * @example {{{ kget com.shocktrade.alerts 0 3456 }}}
   * @example {{{ kget 3456 }}}
   */
  def getMessage(params: UnixLikeArgs): Either[Option[MessageData], Option[Seq[AvroRecord]]] = {
    // get the arguments
    val (topic, partition, offset) = params.args match {
      case anOffset :: Nil =>
        (for {
          topic <- cursor map (_.topic)
          partition <- cursor map (_.partition)
          offset = parseOffset(anOffset)
        } yield (topic, partition, offset)) getOrElse dieSyntax("kget")
      case aTopic :: aPartition :: anOffset :: Nil =>
        (aTopic, parsePartition(aPartition), parseOffset(anOffset))
      case _ =>
        dieSyntax("kget")
    }

    // generate and return the message
    getMessage(topic, partition, offset, params)
  }

  def getMessage(topic: String, partition: Int, offset: Long, unixArgs: UnixLikeArgs): Either[Option[MessageData], Option[Seq[AvroRecord]]] = {
    // retrieve the message
    val messageData = new KafkaSubscriber(TopicSlice(topic, partition), brokers, correlationId) use (
      _.fetch(offset.toLong, defaultFetchSize).headOption)

    // write the data to an output file?
    for {
      path <- unixArgs("-f") map expandPath
      message <- messageData map(_.message)
    } new FileOutputStream(path) use (_.write(message))

    // decode the message using an Avro decoder?
    val schemaVar = unixArgs("-a")
    val avroDecoder = schemaVar map getAvroDecoder
    val decodedMessage = decodeArvoMessage(messageData, schemaVar, avroDecoder)

    // setup the cursor
    val encoding = schemaVar map AvroMessageEncoding getOrElse BinaryMessageEncoding
    cursor = messageData map (m => MessageCursor(topic, partition, m.offset, m.nextOffset, encoding))

    // return either a binary encoded message or an Avro message
    if (decodedMessage.isDefined) Right(decodedMessage) else Left(messageData)
  }

  private def decodeArvoMessage(messageData: Option[MessageData], schemaVar: Option[String], avroDecoder: Option[AvroDecoder]): Option[Seq[AvroRecord]] = {
    for {
      md <- messageData
      schema <- schemaVar
      decoder <- avroDecoder
      rec = decoder.decode(md.message) match {
        case Success(record) =>
          val fields = record.getSchema.getFields.asScala.map(_.name.trim).toSeq
          fields map { f =>
            val v = record.get(f)
            AvroRecord(f, v, Option(v) map (_.getClass.getSimpleName) getOrElse "")
          }
        case Failure(e) =>
          throw new IllegalStateException(e.getMessage, e)
      }
    } yield rec
  }

  /**
   * "kgetsize" - Returns the size of the message for a given topic partition and offset
   * @example {{{ kgetsize com.shocktrade.alerts 0 5567 }}}
   * @example {{{ kgetsize 5567 }}}
   */
  def getMessageSize(params: UnixLikeArgs): Option[Int] = {
    // get the arguments (topic, partition, groupId and offset)
    val (topic, partition, offset) = params.args match {
      case anOffset :: Nil =>
        cursor map (c => (c.topic, c.partition, parseOffset(anOffset))) getOrElse dieNoCursor
      case aTopic :: aPartition :: anOffset :: Nil =>
        (aTopic, parsePartition(aPartition), parseOffset(anOffset))
      case _ =>
        dieSyntax("kgetsize")
    }

    // get the optional arguments
    val fetchSize = params("-s") map (parseInt("fetchSize", _)) getOrElse defaultFetchSize

    // perform the action
    new KafkaSubscriber(TopicSlice(topic, partition), brokers, correlationId) use {
      _.fetch(offset.toLong, fetchSize).headOption map (_.message.length)
    }
  }

  /**
   * "kgetminmax" - Returns the minimum and maximum message size for a given topic partition and offset range
   * @example {{{ kgetmaxsize com.shocktrade.alerts 0 2100 5567 }}}
   * @example {{{ kgetmaxsize 2100 5567 }}}
   */
  def getMessageMinMaxSize(params: UnixLikeArgs): Seq[MessageMaxMin] = {
    // get the arguments (topic, partition, startOffset and endOffset)
    val (topic, partition, startOffset, endOffset) = params.args match {
      case aStartOffset :: anEndOffset :: Nil =>
        cursor map (c => (c.topic, c.partition, parseOffset(aStartOffset), parseOffset(anEndOffset))) getOrElse dieNoCursor
      case aTopic :: aPartition :: aStartOffset :: anEndOffset :: Nil =>
        (aTopic, parsePartition(aPartition), parseOffset(aStartOffset), parseOffset(anEndOffset))
      case _ =>
        dieSyntax("kgetminmax")
    }

    // get the optional arguments
    val fetchSize = params("-s") map (parseInt("fetchSize", _)) getOrElse defaultFetchSize

    // perform the action
    new KafkaSubscriber(TopicSlice(topic, partition), brokers, correlationId) use { subscriber =>
      val offsets = startOffset.toLong to endOffset.toLong
      val messages = subscriber.fetch(offsets, fetchSize).map(_.message.length)
      if (messages.nonEmpty) Seq(MessageMaxMin(messages.min, messages.max)) else Seq.empty
    }
  }

  case class MessageMaxMin(minimumSize: Int, maximumSize: Int)

  /**
   * "knext" - Optionally returns the next message
   * @example {{{ knext }}}
   */
  def getNextMessage(params: UnixLikeArgs)(implicit out: PrintStream) = {
    cursor map { case MessageCursor(topic, partition, offset, nextOffset, encoding) =>
      getMessage(topic, partition, nextOffset, params)
    }
  }

  /**
   * "koffset" - Returns the offset at a specific instant-in-time for a given topic
   * @example {{{ koffset com.shocktrade.alerts 0 -d 2014-05-14T14:30:11 }}}
   * @example {{{ koffset -d 2014-05-14T14:30:11 }}}
   */
  def getOffset(params: UnixLikeArgs): Option[Long] = {
    // get the arguments (topic and partition)
    val (topic, partition) = getTopicAndPartition(params.args)

    // date parser instance
    val sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss")

    // get the arguments
    val sysTimeMillis = params("-d") map (sdf.parse(_).getTime) getOrElse -1L

    // perform the action
    new KafkaSubscriber(TopicSlice(topic, partition), brokers, correlationId) use (_.getOffsetsBefore(sysTimeMillis))
  }

  /**
   * "kprev" - Optionally returns the previous message
   * @example {{{ kprev }}}
   */
  def getPreviousMessage(params: UnixLikeArgs)(implicit out: PrintStream): Option[Any] = {
    cursor map { case MessageCursor(topic, partition, offset, nextOffset, encoding) =>
      getMessage(topic, partition, Math.max(0, offset - 1), params)
    }
  }

  /**
   * "kreplicas" - Lists all replicas for all or a subset of topics
   * @example {{{ kreplicas com.shocktrade.quotes.realtime  }}}
   */
  def getReplicas(params: UnixLikeArgs): Seq[TopicReplicas] = {
    val prefix = params.args.headOption

    KafkaSubscriber.getTopicList(brokers, correlationId) flatMap { t =>
      t.replicas map { replica =>
        TopicReplicas(t.topic, t.partitionId, replica.toString, t.isr.contains(replica))
      } filter (t => prefix.isEmpty || prefix.exists(t.topic.startsWith))
    }
  }

  /**
   * "kstats" - Returns the number of available messages for a given topic
   * @example {{{ kstats com.shocktrade.alerts 0 4 }}}
   * @example {{{ kstats }}}
   */
  def getStatistics(params: UnixLikeArgs): Iterable[TopicOffsets] = {
    // interpret based on the input arguments
    val results = params.args match {
      case Nil =>
        val topic = cursor map (_.topic) getOrElse dieNoCursor
        val partitions = KafkaSubscriber.getTopicList(brokers, correlationId).filter(_.topic == topic).map(_.partitionId)
        if (partitions.nonEmpty) Some((topic, partitions.min, partitions.max)) else None

      case topic :: Nil =>
        val partitions = KafkaSubscriber.getTopicList(brokers, correlationId).filter(_.topic == topic).map(_.partitionId)
        if (partitions.nonEmpty) Some((topic, partitions.min, partitions.max)) else None

      case topic :: aPartition :: Nil =>
        Some((topic, parsePartition(aPartition), parsePartition(aPartition)))

      case topic :: partitionA :: partitionB :: Nil =>
        Some((topic, parsePartition(partitionA), parsePartition(partitionB)))

      case _ =>
        dieSyntax("kstats")
    }

    results match {
      case Some((topic, partition0, partition1)) =>
        if (cursor.isEmpty) {
          cursor = getFirstOffset(topic, partition0) ?? getLastOffset(topic, partition0) map (offset =>
            MessageCursor(topic, partition0, offset, offset + 1, BinaryMessageEncoding))
        }
        getStatisticsData(topic, partition0, partition1)
      case _ => Seq.empty
    }
  }

  /**
   * Generates statistics for the partition range of a given topic
   * @param topic the given topic (e.g. com.shocktrade.quotes.realtime)
   * @param partition0 the starting partition
   * @param partition1 the ending partition
   * @return an iteration of statistics
   */
  private def getStatisticsData(topic: String, partition0: Int, partition1: Int): Iterable[TopicOffsets] = {
    for {
      partition <- partition0 to partition1
      first <- getFirstOffset(topic, partition)
      last <- getLastOffset(topic, partition)
    } yield TopicOffsets(topic, partition, first, last, Math.max(0, last - first))
  }

  /**
   * "kls" - Lists all existing topicList
   */
  def getTopics(params: UnixLikeArgs): Seq[TopicDetail] = {
    val prefix = params.args.headOption

    KafkaSubscriber.getTopicList(brokers, correlationId) flatMap { t =>
      val detail = TopicDetail(t.topic, t.partitionId, t.leader map (_.toString) getOrElse "N/A", t.replicas.size, t.isr.size)
      if (prefix.isEmpty || prefix.exists(t.topic.startsWith)) Some(detail) else None
    }
  }

  /**
   * "kimport" - Imports a message into a new/existing topic
   * @example {{{ kimport com.shocktrade.alerts -text messages/mymessage.txt }}}
   */
  def importMessages(params: UnixLikeArgs): Int = {
    // get the arguments
    val Seq(topic, _*) = params.args

    // expand the file path
    val filePath = params("-f") map expandPath getOrElse dieNoInputSource
    val fileType = params("-a") ?? params("-b") ?? params("-t") getOrElse "-b"

    KafkaPublisher(brokers) use { publisher =>
      publisher.open()

      // import the messages based on file type
      fileType match {
        case "-a" =>
          importMessagesFromAvroFile(publisher, filePath)
        case "-b" =>
          importMessagesFromBinaryFile(publisher, topic, filePath)
        case "-t" =>
          importMessagesFromTextFile(publisher, topic, filePath)
        case unknown =>
          throw new IllegalArgumentException(s"Unrecognized file type '$unknown'")
      }
    }
  }

  /**
   * Imports Avro messages from the given file path
   * @param publisher the given Kafka publisher
   * @param filePath the given file path
   */
  private def importMessagesFromAvroFile(publisher: KafkaPublisher, filePath: String): Int = {
    import org.apache.avro.file.DataFileReader
    import org.apache.avro.generic.{GenericDatumReader, GenericRecord}

    var messages = 0
    val reader = new DataFileReader[GenericRecord](new File(filePath), new GenericDatumReader[GenericRecord]())
    while (reader.hasNext) {
      val record = reader.next()
      (Option(record.get("topic")) map (_.toString)).foreach { case topic =>
        for {
          partition <- Option(record.get("partition"))
          offset <- Option(record.get("offset")) map (v => toBytes(v.asInstanceOf[Long]))
          buf <- Option(record.get("message")) map (_.asInstanceOf[java.nio.Buffer])
          message = buf.array().asInstanceOf[Array[Byte]]
        } {
          publisher.publish(topic, offset, message)
          messages += 1
        }
      }
    }
    messages
  }

  /**
   * Imports text messages from the given file path
   * @param publisher the given Kafka publisher
   * @param filePath the given file path
   */
  private def importMessagesFromBinaryFile(publisher: KafkaPublisher, topic: String, filePath: String): Int = {
    import java.io.{DataInputStream, FileInputStream}

    var messages = 0
    new DataInputStream(new FileInputStream(filePath)) use { in =>
      // get the next message length and retrieve the message
      val messageLength = in.readInt()
      val message = new Array[Byte](messageLength)
      in.read(message, 0, message.length)

      // publish the message
      publisher.publish(topic, toBytes(System.currentTimeMillis()), message)
      messages += 1
    }
    messages
  }

  /**
   * Imports text messages from the given file path
   * @param publisher the given Kafka publisher
   * @param filePath the given file path
   */
  private def importMessagesFromTextFile(publisher: KafkaPublisher, topic: String, filePath: String): Int = {
    import scala.io.Source

    var messages = 0
    Source.fromFile(filePath).getLines() foreach { message =>
      publisher.publish(topic, toBytes(System.currentTimeMillis()), message.getBytes(rt.encoding))
      messages += 1
    }
    messages
  }

  /**
   * "kinbound" - Retrieves a list of all topics with new messages (since last query)
   * @example {{{ kinbound com.shocktrade.quotes }}}
   */
  def inboundMessages(params: UnixLikeArgs): Iterable[Inbound] = {
    val prefix = params.args.headOption

    // is this the initial call to this command?
    if (incomingMessageCache.isEmpty || (System.currentTimeMillis() - lastInboundCheck) >= 15.minutes) {
      out.println("Sampling data; this may take a few seconds...")

      // generate some data to fill the cache
      inboundMessageStatistics()

      // wait 3 second
      Thread.sleep(3 second)
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
    val topics = KafkaSubscriber.getTopicList(brokers, correlationId)
      .filter(t => t.topic == topicPrefix.getOrElse(t.topic))
      .groupBy(_.topic)

    // generate the inbound data
    val inboundData = (topics flatMap { case (topic, details) =>
      // get the range of partitions for each topic
      val partitions = details.map(_.partitionId)
      val (beginPartition, endPartition) = (partitions.min, partitions.max)

      // retrieve the statistics for each topic
      getStatisticsData(topic, beginPartition, endPartition) map { o =>
        val prevInbound = incomingMessageCache.get(TopicSlice(o.topic, o.partition))
        val lastCheckTime = prevInbound.map(_.lastCheckTime.getTime) getOrElse System.currentTimeMillis()
        val currentTime = System.currentTimeMillis()
        val elapsedTime = 1 + (currentTime - lastCheckTime) / 1000L
        val change = prevInbound map (o.endOffset - _.endOffset) getOrElse 0L
        val rate = BigDecimal(change.toDouble / elapsedTime).setScale(1, BigDecimal.RoundingMode.UP).toDouble
        Inbound(o.topic, o.partition, o.startOffset, o.endOffset, change, rate, new Date(currentTime))
      }
    }).toSeq

    // cache the unfiltered inbound data
    incomingMessageCache = incomingMessageCache ++ Map(inboundData map (i => TopicSlice(i.topic, i.partition) -> i): _*)

    // filter out the non-changed records
    inboundData.filterNot(_.change == 0) sortBy (-_.change)
  }

  case class Inbound(topic: String, partition: Int, startOffset: Long, endOffset: Long, change: Long, msgsPerSec: Double, lastCheckTime: Date)

  case class TopicReplicas(topic: String, partition: Int, replicaBroker: String, inSync: Boolean)

  /**
   * "kpublish" - Returns the EOF offset for a given topic
   */
  def publishMessage(params: UnixLikeArgs)(implicit out: PrintStream): Unit = {
    // get the arguments
    val (topic, key, message) = params.args match {
      case aKey :: aMessage :: Nil => cursor map (c => (c.topic, aKey, aMessage)) getOrElse dieNoCursor
      case aTopic :: aGroupId :: aKey :: aMessage :: Nil => (aTopic, aKey, aMessage)
      case _ => dieSyntax("kpublish")
    }

    // convert the key and message to binary
    val keyBytes = toBinary(key)
    val msgBytes = toBinary(message)

    // publish the message
    KafkaPublisher(brokers) use { publisher =>
      publisher.open()
      publisher.publish(topic, keyBytes, msgBytes)
    }
  }

  /**
   * "kreset" - Sets the offset of a consumer group ID to zero for all partitions
   * @example {{{ kreset com.shocktrade.quotes.csv lld }}}
   */
  def resetConsumerGroup(params: UnixLikeArgs): Unit = {
    // get the arguments
    val (topic, groupId) = params.args match {
      case aGroupId :: Nil => cursor map (c => (c.topic, aGroupId)) getOrElse dieNoCursor
      case aTopic :: aGroupId :: Nil => (aTopic, aGroupId)
      case _ => dieSyntax("kreset")
    }

    // get the partition range
    val partitions = KafkaSubscriber.getTopicList(brokers, correlationId) filter (_.topic == topic) map (_.partitionId)
    if (partitions.isEmpty)
      throw new IllegalStateException(s"No partitions found for topic $topic")
    val (start, end) = (partitions.min, partitions.max)

    // reset the consumer group ID for each partition
    (start to end) foreach { partition =>
      new KafkaSubscriber(TopicSlice(topic, partition), brokers, correlationId = 0) use { subscriber =>
        subscriber.commitOffsets(groupId, offset = 0L, "resetting consumer ID")
      }
    }
  }

  private def die[S](message: String): S = throw new IllegalArgumentException(message)

  private def dieNoCursor[S](): S = die("No cursor exists")

  private def dieNoInputSource[S](): S = die("No input source specified")

  private def dieNoOutputSource[S](): S = die("No output source specified")

  private def dieSyntax[S](command: String): S = die( s"""Invalid arguments - use "syntax $command" to see usage""")

  private def parsePartition(partition: String): Int = parseInt("partition", partition)

  private def parseOffset(offset: String): Long = parseLong("offset", offset)

  private def getPartitionRange(topic: String): (Int, Int) = {
    val partitions = KafkaSubscriber.getTopicList(brokers, correlationId) filter (_.topic == topic) map (_.partitionId)
    if (partitions.isEmpty)
      throw new IllegalStateException(s"No partitions found for topic $topic")
    (partitions.min, partitions.max)
  }

  /**
   * Retrieves the topic and partition from the given arguments
   * @param args the given arguments
   * @return a tuple containing the topic and partition
   */
  private def getTopicAndPartition(args: Seq[String]): (String, Int) = {
    args.toList match {
      case Nil => cursor map (c => (c.topic, c.partition)) getOrElse dieNoCursor
      case aTopic :: Nil => (aTopic, 0)
      case aTopic :: aPartition :: Nil => (aTopic, parsePartition(aPartition))
      case _ => die("Invalid arguments")
    }
  }

  /**
   * Converts a binary string to a byte array
   * @param hex the given binary string (e.g. "de.ad.be.ef.00")
   * @return a byte array
   */
  private def toBinary(hex: String): Array[Byte] = hex.split("[.]") map (Integer.parseInt(_, 16)) map (_.toByte)

  /**
   * Converts the given long value into a byte array
   * @param value the given long value
   * @return a byte array
   */
  private def toBytes(value: Long): Array[Byte] = allocate(8).putLong(value).array()

  case class AvroRecord(field: String, value: Any, `type`: String)

  case class AvroVerification(verified: Int, failed: Int)

  case class MessageCursor(topic: String, partition: Int, offset: Long, nextOffset: Long, encoding: MessageEncoding)

  case class TopicDetail(topic: String, partition: Int, leader: String, replicas: Int, inSync: Int)

  case class TopicOffsets(topic: String, partition: Int, startOffset: Long, endOffset: Long, messagesAvailable: Long)

}

/**
 * Kafka Module Companion Object
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
object KafkaModule {

  sealed trait MessageEncoding

  case class AvroMessageEncoding(schemaVarName: String) extends MessageEncoding {
    override def toString = s"Avro:$schemaVarName"
  }

  case object BinaryMessageEncoding extends MessageEncoding {
    override def toString = s"Binary"
  }

  /**
   * Binary Key Equality Condition
   */
  case class BinaryKeyEqCondition(mykey: Array[Byte]) extends Condition {
    override def satisfies(message: Array[Byte], key: Option[Array[Byte]]) = key.exists(_ sameElements mykey)
  }

}
