package com.ldaniels528.verify.modules.kafka

import java.io.{File, PrintStream}
import java.nio.ByteBuffer._
import java.text.SimpleDateFormat
import java.util.Date

import com.ldaniels528.verify.VxRuntimeContext
import com.ldaniels528.verify.io.Compression
import com.ldaniels528.verify.modules.avro.AvroReading
import com.ldaniels528.verify.modules.kafka.KafkaModule._
import com.ldaniels528.verify.modules.kafka.KafkaSubscriber.{BrokerDetails, MessageData}
import com.ldaniels528.verify.modules.{Command, Module}
import com.ldaniels528.verify.util.BinaryMessaging
import com.ldaniels528.verify.util.VxUtils._
import com.ldaniels528.verify.vscript.VScriptRuntime.ConstantValue
import com.ldaniels528.verify.vscript.{Scope, Variable}

import scala.concurrent.ExecutionContext.Implicits._
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.{Failure, Success, Try}

/**
 * Kafka Module
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class KafkaModule(rt: VxRuntimeContext) extends Module with BinaryMessaging with AvroReading with Compression {
  private implicit val out: PrintStream = rt.out
  private implicit val scope: Scope = rt.scope
  private implicit val rtc: VxRuntimeContext = rt

  // date parser instance
  private val sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss")

  // create the ZooKeeper proxy
  private implicit val zk = rt.zkProxy

  // get the list of brokers from zookeeper
  private val brokers: Seq[Broker] = KafkaSubscriber.getBrokerList(zk) map (b => Broker(b.host, b.port))

  // set the default correlation ID
  private val correlationId: Int = (Math.random * Int.MaxValue).toInt

  // incoming messages cache
  private var incomingMessageCache = Map[Topic, Inbound]()
  private var lastInboundCheck: Long = _

  // define the offset for kget/knext
  private var cursor: Option[MessageCursor] = None

  def defaultFetchSize = scope.getValue[Int]("defaultFetchSize") getOrElse 65536

  def defaultFetchSize_=(sizeInBytes: Int) = scope.setValue("defaultFetchSize", Option(sizeInBytes))

  // the bound commands
  override def getCommands: Seq[Command] = Seq(
    Command(this, "kbrokers", getBrokers, (Seq.empty, Seq.empty), help = "Returns a list of the brokers from ZooKeeper"),
    Command(this, "kcommit", commitOffset, (Seq("topic", "partition", "groupId", "offset"), Seq("metadata")), "Commits the offset for a given topic and group"),
    Command(this, "kconsumers", getConsumers, (Seq.empty, Seq("topicPrefix")), help = "Returns a list of the consumers from ZooKeeper"),
    Command(this, "kcursor", getCursor, (Seq.empty, Seq.empty), help = "Displays the current message cursor"),
    Command(this, "kexport", exportToFile, (Seq("file", "topic", "partition"), Seq("startOffset", "endOffset", "flags", "blockSize")), "Writes the contents of a specific topic to a file"),
    Command(this, "kfetch", fetchOffsets, (Seq("topic", "partition", "groupId"), Seq.empty), "Retrieves the offset for a given topic and group"),
    Command(this, "kfetchsize", fetchSizeGetOrSet, (Seq.empty, Seq("fetchSize")), help = "Retrieves or sets the default fetch size for all Kafka queries"),
    Command(this, "kfirst", getFirstMessage, (Seq.empty, Seq("topic", "partition")), help = "Returns the first message for a given topic"),
    Command(this, "kget", getMessage, (Seq("topic", "partition", "offset"), Seq("fetchSize")), help = "Retrieves the message at the specified offset for a given topic partition"),
    Command(this, "kgeta", getMessageAvro, (Seq("schemaPath", "topic", "partition"), Seq("offset", "blockSize")), help = "Returns the key-value pairs of an Avro message from a topic partition"),
    Command(this, "kgetsize", getMessageSize, (Seq("topic", "partition", "offset"), Seq("fetchSize")), help = "Retrieves the size of the message at the specified offset for a given topic partition"),
    Command(this, "kgetminmax", getMessageMinMaxSize, (Seq("topic", "partition", "startOffset", "endOffset"), Seq("fetchSize")), help = "Retrieves the smallest and largest message sizes for a range of offsets for a given partition"),
    Command(this, "kimport", importMessages, (Seq("topic", "fileType", "filePath"), Seq.empty), "Imports messages into a new/existing topic"),
    Command(this, "kinbound", inboundMessages, (Seq.empty, Seq("topicPrefix")), "Retrieves a list of topics with new messages (since last query)"),
    Command(this, "klast", getLastMessage, (Seq.empty, Seq("topic", "partition")), help = "Returns the last message for a given topic"),
    Command(this, "kls", getTopics, (Seq.empty, Seq("topicPrefix")), help = "Lists all existing topics"),
    Command(this, "knext", getNextMessage, (Seq.empty, Seq.empty), "Attempts to retrieve the next message"),
    Command(this, "koffset", getOffset, (Seq("topic", "partition"), Seq("time=YYYY-MM-DDTHH:MM:SS")), "Returns the offset at a specific instant-in-time for a given topic"),
    Command(this, "kprev", getPreviousMessage, (Seq.empty, Seq.empty), "Attempts to retrieve the message at the previous offset"),
    Command(this, "kpublish", publishMessage, (Seq("topic", "key"), Seq.empty), "Publishes a message to a topic"),
    Command(this, "kreplicas", getReplicas, (Seq.empty, Seq("prefix")), help = "Returns a list of replicas for specified topics"),
    Command(this, "kscana", scanMessagesAvro, (Seq("schemaPath", "topic", "partition", "startOffset", "endOffset"), Seq("batchSize", "blockSize")), help = "Scans a range of messages verifying conformance to an Avro schema"),
    Command(this, "ksearch", findMessageByKey, (Seq.empty, Seq("topic", "groupId", "keyVariable")), help = "Scans a topic for a message with a given key"),
    Command(this, "kstats", getStatistics, (Seq.empty, Seq("topic", "beginPartition", "endPartition")), help = "Returns the partition details for a given topic"))

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
  def commitOffset(args: String*): Option[Short] = {
    // get the arguments
    val Seq(name, partition, groupId, offset, _*) = args
    val metadata = extract(args, 4) getOrElse ""

    // perform the action
    new KafkaSubscriber(Topic(name, parseInt("partition", partition)), brokers, correlationId) use (_.commitOffsets(groupId, offset.toLong, metadata))
  }

  /**
   * "kexport" - Dumps the contents of a specific topic to a file
   * @example {{{ kexport quotes.txt com.shocktrade.quotes.csv 12388 16235 }}}
   */
  def exportToFile(args: String*): Long = {
    import java.io.{DataOutputStream, FileOutputStream}

    // get the arguments
    val Seq(file, name, partition, _*) = args
    val startOffset = extract(args, 3) map (parseLong("startOffset", _))
    val endOffset = extract(args, 4) map (parseLong("endOffset", _))
    val blockSize = extract(args, 5) map (parseInt("blockSize", _))

    // output the output file
    var count = 0L
    new DataOutputStream(new FileOutputStream(file)) use { fos =>
      // perform the action
      new KafkaSubscriber(Topic(name, parseInt("partition", partition)), brokers, correlationId) use {
        _.consume(startOffset, endOffset, blockSize, listener = new MessageConsumer {
          override def consume(offset: Long, nextOffset: Option[Long], message: Array[Byte]) {
            fos.writeInt(message.length)
            fos.write(message)
            count += 1
          }
        })
      }
    }
    count
  }

  /**
   * "kfetch" - Returns the offsets for a given topic and group ID
   * @example {{{ kfetch com.shocktrade.alerts 0 devc }}}
   */
  def fetchOffsets(args: String*): Option[Long] = {
    // get the arguments
    val Seq(name, partition, groupId, _*) = args

    // perform the action
    new KafkaSubscriber(Topic(name, parseInt("partition", partition)), brokers, correlationId) use (_.fetchOffsets(groupId))
  }

  /**
   * "kfetchsize" - Retrieves or sets the default fetch size for all Kafka queries
   * @param args the given command line arguments
   * @return
   */
  def fetchSizeGetOrSet(args: String*) = {
    args.headOption match {
      case Some(fetchSize) => defaultFetchSize = fetchSize.toInt
      case None => defaultFetchSize
    }
  }

  /**
   * "ksearch" - Scans a topic for a message with a given key
   * @example {{{ ksearch com.shocktrade.quotes.csv devGroup myKey }}}
   */
  def findMessageByKey(args: String*): Future[Option[MessageData]] = {
    // get the topic and partition arguments
    val (topic, groupId, keyVar) = args.toList match {
      case groupIdArg :: keyArg :: Nil =>
        cursor map (c => (c.topic, groupIdArg, keyArg)) getOrElse die("No cursor exists")
      case topicArg :: groupIdArg :: keyArg :: Nil => (topicArg, groupIdArg, keyArg)
      case _ => die( s"""Invalid arguments - use "syntax ksearch" to see usage""")
    }

    // get the binary key
    val variable = scope.getVariable(keyVar) getOrElse die(s"Variable '$keyVar' not found")
    val key = variable.eval[Array[Byte]] getOrElse die(s"$keyVar is undefined")

    // get the consumer instance
    val consumer = KafkaStreamingConsumer(rt.zkEndPoint, groupId)

    // perform the search
    val result = consumer.scan(topic, parallelism = 4, key) map (_ map { msg =>
      val lastOffset: Long = getLastOffset(msg.topic, msg.partition) getOrElse -1L
      val nextOffset: Long = msg.offset + 1
      cursor = Option(MessageCursor(msg.topic, msg.partition, msg.offset, nextOffset, BINARY))
      MessageData(msg.offset, nextOffset, lastOffset, msg.message)
    })

    // close the consumer once a response is available
    result.foreach(msg_? => consumer.close())
    result
  }

  /**
   * "kbrokers" - Retrieves the list of Kafka brokers
   */
  def getBrokers(args: String*): Seq[BrokerDetails] = KafkaSubscriber.getBrokerList

  /**
   * "kconsumers" - Retrieves the list of Kafka consumers
   */
  def getConsumers(args: String*): Seq[ConsumerDelta] = {
    // get the optional topic prefix
    val topicPrefix = args.headOption

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
   */
  def getCursor(args: String*): Seq[MessageCursor] = cursor.map(c => Seq(c)) getOrElse Seq.empty

  /**
   * "kfirst" - Returns the first message for a given topic
   * @example {{{ kfirst com.shocktrade.quotes.csv 0 }}}
   */
  def getFirstMessage(args: String*): Option[MessageData] = {
    // get the arguments
    val (topic, partition) = getTopicAndPartition(args)

    // perform the action
    new KafkaSubscriber(Topic(topic, partition), brokers, correlationId) use { subscriber =>
      subscriber.getFirstOffset flatMap { offset =>
        val messageData = subscriber.fetch(offset, defaultFetchSize).headOption
        cursor = messageData map (m => MessageCursor(topic, partition, m.offset, m.nextOffset, BINARY))
        messageData
      }
    }
  }

  /**
   * Returns the first offset for a given topic
   */
  def getFirstOffset(topic: String, partition: Int): Option[Long] = {
    new KafkaSubscriber(Topic(topic, partition), brokers, correlationId) use (_.getFirstOffset)
  }

  /**
   * "klast" - Returns the last offset for a given topic
   */
  def getLastMessage(args: String*): Option[MessageData] = {
    // get the arguments
    val (topic, partition) = getTopicAndPartition(args)

    // perform the action
    new KafkaSubscriber(Topic(topic, partition), brokers, correlationId) use { subscriber =>
      subscriber.getLastOffset flatMap { offset =>
        val messageData = subscriber.fetch(offset, defaultFetchSize).headOption
        cursor = messageData map (m => MessageCursor(topic, partition, m.offset, m.nextOffset, BINARY))
        messageData
      }
    }
  }

  /**
   * Returns the last offset for a given topic
   */
  def getLastOffset(topic: String, partition: Int): Option[Long] = {
    new KafkaSubscriber(Topic(topic, partition), brokers, correlationId) use (_.getLastOffset)
  }

  /**
   * "kgeta" - Returns the key-value pairs of an Avro message from a Kafka partition
   * @example {{{ kavrofields avro/schema1.avsc com.shocktrade.alerts 0 58500700 }}}
   * @example {{{ kavrofields avro/schema2.avsc com.shocktrade.alerts 9 1799020 }}}
   */
  def getMessageAvro(args: String*)(implicit out: PrintStream): Seq[AvroRecord] = {
    import scala.collection.JavaConverters._

    // get the arguments
    val Seq(schemaVar, name, partition, _*) = args
    val offset = extract(args, 3) map (parseLong("offset", _))
    val blockSize = extract(args, 4) map (parseInt("blockSize", _))

    // get the decoder
    val decoder = getAvroDecoder(schemaVar)

    // perform the action
    var results: Seq[AvroRecord] = Nil
    new KafkaSubscriber(Topic(name, parseInt("partition", partition)), brokers, correlationId) use {
      _.consume(offset, offset map (_ + 1), blockSize, listener = new MessageConsumer {
        override def consume(offset: Long, nextOffset: Option[Long], message: Array[Byte]) {
          decoder.decode(message) match {
            case Success(record) =>
              cursor = nextOffset map (nextOffset => MessageCursor(name, partition.toInt, offset, nextOffset, AVRO))
              val fields = record.getSchema.getFields.asScala.map(_.name.trim).toSeq
              results = fields map { f =>
                val v = record.get(f)
                AvroRecord(f, v, Option(v) map (_.getClass.getSimpleName) getOrElse "")
              }
            case Failure(e) =>
              out.println("[%04d] %s".format(offset, e.getMessage))
          }
        }
      })
    }

    results
  }

  /**
   * "kget" - Returns the message for a given topic partition and offset
   * @example {{{ kget com.shocktrade.alerts 0 45913975 }}}
   */
  def getMessage(args: String*)(implicit out: PrintStream): Option[MessageData] = {
    // get the arguments
    val Seq(name, partition, offset, _*) = args
    val fetchSize = extract(args, 3) map (parseInt("fetchSize", _)) getOrElse defaultFetchSize

    // perform the action
    var messageData: Option[MessageData] = None
    new KafkaSubscriber(Topic(name, parseInt("partition", partition)), brokers, correlationId) use { subscriber =>
      messageData = subscriber.fetch(offset.toLong, fetchSize).headOption
      cursor = messageData map (m => MessageCursor(name, partition.toInt, m.offset, m.nextOffset, BINARY))
    }
    messageData
  }

  case class MessageCursor(topic: String, partition: Int, offset: Long, nextOffset: Long, encoding: MessageEncoding)

  /**
   * "kgetsize" - Returns the size of the message for a given topic partition and offset
   * @example {{{ kgetsize com.shocktrade.alerts 0 45913975 }}}
   */
  def getMessageSize(args: String*): Option[Int] = {
    // get the arguments
    val Seq(name, partition, offset, _*) = args
    val fetchSize = extract(args, 3) map (parseInt("fetchSize", _)) getOrElse defaultFetchSize

    // perform the action
    new KafkaSubscriber(Topic(name, parseInt("partition", partition)), brokers, correlationId) use {
      _.fetch(offset.toLong, fetchSize).headOption map (_.message.length)
    }
  }

  /**
   * "kgetminmax" - Returns the minimum and maximum message size for a given topic partition and offset range
   * @example {{{ kgetmaxsize com.shocktrade.alerts 0 45913900 45913975 }}}
   */
  def getMessageMinMaxSize(args: String*): Seq[MessageMaxMin] = {
    // get the arguments
    val Seq(name, partition, startOffset, endOffset, _*) = args
    val fetchSize = extract(args, 4) map (parseInt("fetchSize", _)) getOrElse defaultFetchSize

    // perform the action
    new KafkaSubscriber(Topic(name, parseInt("partition", partition)), brokers, correlationId) use { subscriber =>
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
  def getNextMessage(args: String*)(implicit out: PrintStream): Option[Any] = {
    cursor map { case MessageCursor(topic, partition, offset, nextOffset, encoding) =>
      encoding match {
        case BINARY =>
          getMessage(Seq(topic, partition.toString, nextOffset.toString): _*)
        case AVRO =>
          getMessageAvro(Seq(topic, partition.toString, nextOffset.toString): _*)
        case unknown =>
          throw new IllegalStateException(s"Unrecognized encoding $unknown")
      }
    }
  }

  /**
   * "koffset" - Returns the offset at a specific instant-in-time for a given topic
   * @example {{{ koffset com.shocktrade.alerts 0 2014-05-14T14:30:11 }}}
   */
  def getOffset(args: String*): Option[Long] = {
    // get the arguments
    val Seq(name, partition, _*) = args
    val sysTimeMillis = extract(args, 2) map (sdf.parse(_).getTime) getOrElse -1L

    // perform the action
    new KafkaSubscriber(Topic(name, parseInt("partition", partition)), brokers, correlationId) use (_.getOffsetsBefore(sysTimeMillis))
  }

  /**
   * "kprev" - Optionally returns the previous message
   * @example {{{ kprev }}}
   */
  def getPreviousMessage(args: String*)(implicit out: PrintStream): Option[Any] = {
    cursor map { case MessageCursor(topic, partition, offset, nextOffset, encoding) =>
      encoding match {
        case BINARY =>
          getMessage(Seq(topic, partition.toString, (offset - 1).toString): _*)
        case AVRO =>
          getMessageAvro(Seq(topic, partition.toString, (offset - 1).toString): _*)
        case unknown =>
          throw new IllegalStateException(s"Unrecognized encoding $unknown")
      }
    }
  }

  /**
   * "kstats" - Returns the number of available messages for a given topic
   * @example {{{ kstats com.shocktrade.alerts 0 4 }}}
   */
  def getStatistics(args: String*): Iterable[TopicOffsets] = {
    // interpret based on the input arguments
    val results = args.toList match {
      case Nil =>
        val topic = cursor map (_.topic) getOrElse die("No cursor exists")
        val partitions = KafkaSubscriber.getTopicList(brokers, correlationId).filter(_.topic == topic).map(_.partitionId)
        if (partitions.nonEmpty) Some((topic, partitions.min, partitions.max)) else None

      case topic :: Nil =>
        val partitions = KafkaSubscriber.getTopicList(brokers, correlationId).filter(_.topic == topic).map(_.partitionId)
        if (partitions.nonEmpty) Some((topic, partitions.min, partitions.max)) else None

      case topic :: partition :: Nil =>
        val partitions = KafkaSubscriber.getTopicList(brokers, correlationId).filter(_.topic == topic).map(_.partitionId)
        if (partitions.nonEmpty) Some((topic, parsePartition(partition), partitions.max)) else None

      case topic :: partitionA :: partitionB :: Nil =>
        Some((topic, parsePartition(partitionA), parsePartition(partitionB)))

      case _ =>
        None
    }

    results match {
      case Some((topic, partition0, partition1)) =>
        getStatisticsData(topic, partition0, partition1)
      case _ => Seq.empty
    }
  }

  private def getStatisticsData(topic: String, partition0: Int, partition1: Int): Iterable[TopicOffsets] = {
    for {
      partition <- partition0 to partition1
      first <- getFirstOffset(topic, partition)
      last <- getLastOffset(topic, partition)
    } yield TopicOffsets(topic, partition, first, last, Math.max(0,  last - first))
  }

  /**
   * "kls" - Lists all existing topicList
   */
  def getTopics(args: String*): Seq[TopicDetail] = {
    val prefix = args.headOption

    KafkaSubscriber.getTopicList(brokers, correlationId) flatMap { t =>
      val detail = TopicDetail(t.topic, t.partitionId, t.leader map (_.toString) getOrElse "N/A", t.replicas.size, t.isr.size)
      if (prefix.isEmpty || prefix.exists(t.topic.startsWith)) Some(detail) else None
    }
  }

  /**
   * "kimport" - Imports a message into a new/existing topic
   * @example {{{ kimport com.shocktrade.alerts -text messages/mymessage.txt }}}
   */
  def importMessages(args: String*): Int = {
    // get the arguments
    val Seq(topic, fileType, rawFilePath, _*) = args

    // expand the file path
    val filePath = expandPath(rawFilePath)

    KafkaPublisher(brokers) use { publisher =>
      publisher.open()

      // import the messages based on file type
      fileType.toLowerCase match {
        case "-avro" | "-a" =>
          importMessagesFromAvroFile(publisher, filePath)
        case "-binary" | "-b" =>
          importMessagesFromBinaryFile(publisher, topic, filePath)
        case "-text" | "-t" =>
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
  def inboundMessages(args: String*): Iterable[Inbound] = {
    val prefix = args.headOption

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
      getStatistics(topic, beginPartition.toString, endPartition.toString) map { o =>
        val prevInbound = incomingMessageCache.get(Topic(o.topic, o.partition))
        val lastCheckTime = prevInbound.map(_.lastCheckTime.getTime) getOrElse System.currentTimeMillis()
        val currentTime = System.currentTimeMillis()
        val elapsedTime = 1 + (currentTime - lastCheckTime) / 1000L
        val change = prevInbound map (o.endOffset - _.endOffset) getOrElse 0L
        val rate = BigDecimal(change.toDouble / elapsedTime).setScale(1, BigDecimal.RoundingMode.UP).toDouble
        Inbound(o.topic, o.partition, o.startOffset, o.endOffset, change, rate, new Date(currentTime))
      }
    }).toSeq

    // cache the unfiltered inbound data
    incomingMessageCache = incomingMessageCache ++ Map(inboundData map (i => Topic(i.topic, i.partition) -> i): _*)

    // filter out the non-changed records
    inboundData.filterNot(_.change == 0) sortBy (-_.change)
  }

  case class Inbound(topic: String, partition: Int, startOffset: Long, endOffset: Long, change: Long, msgsPerSec: Double, lastCheckTime: Date)

  /**
   * "kreplicas" - Lists all replicas for all or a subset of topics
   * @example {{{ kreplicas com.shocktrade.quotes.realtime  }}}
   */
  def getReplicas(args: String*): Seq[TopicReplicas] = {
    val prefix = args.headOption

    KafkaSubscriber.getTopicList(brokers, correlationId) flatMap { t =>
      t.replicas map { replica =>
        TopicReplicas(t.topic, t.partitionId, replica.toString, t.isr.contains(replica))
      } filter (t => prefix.isEmpty || prefix.exists(t.topic.startsWith))
    }
  }

  case class TopicReplicas(topic: String, partition: Int, replicaBroker: String, inSync: Boolean)

  /**
   * "kpublish" - Returns the EOF offset for a given topic
   */
  def publishMessage(args: String*)(implicit out: PrintStream): Unit = {
    // get the arguments
    val Seq(name, key, _*) = args

    out.println("Type the message and press ENTER:")
    out.print(">> ")
    val message = Console.readLine().trim

    // publish the message
    KafkaPublisher(brokers) use { publisher =>
      publisher.open()
      publisher.publish(name, toBytes(key.toLong), message.getBytes)
    }
  }

  /**
   * kscana - Scans and verifies that a set of messages (specific offset range) can be read by the specified schema
   * @example {{{ kscana avro/schema1.avsc com.shocktrade.alerts 0 1000 2000 }}}
   */
  def scanMessagesAvro(args: String*)(implicit out: PrintStream): Seq[AvroVerification] = {
    // get the arguments
    val Seq(schemaVar, name, partition, startOffset, endOffset, _*) = args
    val batchSize = extract(args, 5) map (parseInt("batchSize", _)) getOrElse 10
    val blockSize = extract(args, 6) map (parseInt("blockSize", _)) getOrElse defaultFetchSize

    // get the decoder
    val decoder = getAvroDecoder(schemaVar)

    // check all records within the range
    var verified = 0
    var errors = 0
    new KafkaSubscriber(Topic(name, parseInt("partition", partition)), brokers, correlationId) use { subscriber =>
      (startOffset.toLong to endOffset.toLong).sliding(batchSize, batchSize) foreach { offsets =>
        Try(subscriber.fetch(offsets, blockSize)) match {
          case Success(messages) =>
            messages foreach { m =>
              Try(decoder.decode(m.message)) match {
                case Success(_) => verified += 1
                case Failure(e) =>
                  out.println("[%04d] %s".format(m.offset, e.getMessage))
                  errors += 1
              }
            }
          case Failure(e) =>
            out.println(s"!!! %s between offsets %d and %d".format(e.getMessage, offsets.min, offsets.max))
        }
      }
    }

    Seq(AvroVerification(verified, errors))
  }

  private def parsePartition(partition: String): Int = parseInt("partition", partition)

  private def parseOffset(offset: String): Long = parseLong("offset", offset)

  /**
   * "kwatch" - Subscribes to a specific topic
   */
  def watchTopic(args: String*)(implicit out: PrintStream): Long = {
    // get the arguments
    val Seq(name, partition, _*) = args
    val duration = (extract(args, 2) map (parseInt("duration", _)) getOrElse 60).seconds

    // perform the action
    var count = 0L
    KafkaSubscriber.watch(Topic(name, parseInt("partition", partition)), brokers, None, duration, correlationId,
      new MessageConsumer {
        override def consume(offset: Long, nextOffset: Option[Long], message: Array[Byte]) {
          dumpMessage(offset, message)(rt, out)
          count += 1
        }
      })
    count
  }

  /**
   * "kwatchgroup" - Subscribes to a specific topic using a consumer group ID
   */
  def watchTopicWithConsumerGroup(args: String*)(implicit out: PrintStream): Long = {
    // get the arguments
    val Seq(name, partition, groupId, _*) = args
    val duration = (extract(args, 3) map (parseInt("duration", _)) getOrElse 60).seconds

    // perform the action
    var count = 0L
    KafkaSubscriber.watchGroup(Topic(name, parseInt("partition", partition)), brokers, groupId, duration, correlationId,
      new MessageConsumer {
        override def consume(offset: Long, nextOffset: Option[Long], message: Array[Byte]) {
          dumpMessage(offset, message)(rt, out)
          count += 1
        }
      })
    count
  }

  private def die[S](message: String): S = throw new IllegalArgumentException(message)

  /**
   * Retrieves the topic and partition from the given arguments
   * @param args the given arguments
   * @return a tuple containing the topic and partition
   */
  private def getTopicAndPartition(args: Seq[String]): (String, Int) = {
    args.toList match {
      case Nil => cursor map (c => (c.topic, c.partition)) getOrElse die("No cursor exists")
      case topicArg :: Nil => (topicArg, 0)
      case topicArg :: partitionArg :: Nil => (topicArg, parseInt("partition", partitionArg))
      case _ => die("Invalid arguments")
    }
  }

  /**
   * Converts the given long value into a byte array
   * @param value the given long value
   * @return a byte array
   */
  private def toBytes(value: Long): Array[Byte] = allocate(8).putLong(value).array()

  case class AvroRecord(field: String, value: Any, `type`: String)

  case class AvroVerification(verified: Int, failed: Int)

  case class TopicDetail(topic: String, partition: Int, leader: String, replicas: Int, isr: Int)

  case class TopicOffsets(topic: String, partition: Int, startOffset: Long, endOffset: Long, messagesAvailable: Long)

}

/**
 * Kafka Module Companion Object
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
object KafkaModule {

  case class MessageEncoding(value: String) extends AnyVal

  val BINARY = MessageEncoding("Binary")
  val AVRO = MessageEncoding("Avro")

}
