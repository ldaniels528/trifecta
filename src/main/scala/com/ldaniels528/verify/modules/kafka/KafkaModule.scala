package com.ldaniels528.verify.modules.kafka

import java.io.{File, PrintStream}
import java.nio.ByteBuffer._
import java.text.SimpleDateFormat
import java.util.Date

import KafkaModule._
import com.ldaniels528.tabular.Tabular
import com.ldaniels528.verify.VerifyShellRuntime
import com.ldaniels528.verify.io.Compression
import com.ldaniels528.verify.io.avro.{AvroDecoder, AvroTables}
import com.ldaniels528.verify.modules.kafka.KafkaSubscriber.BrokerDetails
import com.ldaniels528.verify.modules.{Command, Module}
import com.ldaniels528.verify.util.VxUtils._
import org.slf4j.LoggerFactory

import scala.concurrent.duration._
import scala.io.Source
import scala.language.postfixOps
import scala.util.{Failure, Success, Try}

/**
 * Kafka Module
 */
class KafkaModule(rt: VerifyShellRuntime) extends Module with Compression {
  private lazy val logger = LoggerFactory.getLogger(getClass)
  private implicit val out: PrintStream = rt.out

  // date parser instance
  private val sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss")

  // define a custom tabular instance
  private val tabular = new Tabular() with AvroTables

  // create the ZooKeeper proxy
  private val zk = rt.zkProxy

  // get the list of brokers from zookeeper
  private val brokers: Seq[Broker] = KafkaSubscriber.getBrokerList(zk) map (b => Broker(b.host, b.port))

  // set the default correlation ID
  private val correlationId: Int = (Math.random * Int.MaxValue).toInt

  // incoming messages cache
  private var incomingMessageCache = Map[Topic, Inbound]()
  private var lastInboundCheck: Long = _

  // define the offset for kget/knext
  private var cursor: Option[MessageCursor] = None

  // the name of the module
  val moduleName = "kafka"

  override def prompt: String = s"${rt.remoteHost}${rt.zkCwd}"

  // the bound commands
  val getCommands = Seq(
    Command(this, "kbrokers", listBrokers, (Seq.empty, Seq.empty), help = "Returns a list of the registered brokers from ZooKeeper"),
    Command(this, "kchka", scanTopicAvro, (Seq("schemaPath", "topic", "partition", "startOffset", "endOffset"), Seq("batchSize", "blockSize")), help = "Verifies that a range of messages can be read by a given Avro schema"),
    Command(this, "kcommit", commitOffset, (Seq("topic", "partition", "groupId", "offset"), Seq("metadata")), "Commits the offset for a given topic and group"),
    Command(this, "kexport", exportToFile, (Seq("file", "topic", "partition"), Seq("startOffset", "endOffset", "flags", "blockSize")), "Writes the contents of a specific topic to a file"),
    Command(this, "kfetch", fetchOffsets, (Seq("topic", "partition", "groupId"), Seq.empty), "Retrieves the offset for a given topic and group"),
    Command(this, "kfetchsize", fetchSizeGetOrSet, (Seq.empty, Seq("fetchSize")), help = "Retrieves or sets the default fetch size for all Kafka queries"),
    Command(this, "kfirst", getFirstOffset, (Seq("topic", "partition"), Seq.empty), help = "Returns the first offset for a given topic"),
    Command(this, "kget", getMessage, (Seq("topic", "partition", "offset"), Seq("fetchSize")), help = "Retrieves the message at the specified offset for a given topic partition"),
    Command(this, "kgeta", getMessageAvro, (Seq("schemaPath", "topic", "partition"), Seq("offset", "blockSize")), help = "Returns the key-value pairs of an Avro message from a topic partition"),
    Command(this, "kgetsize", getMessageSize, (Seq("topic", "partition", "offset"), Seq("fetchSize")), help = "Retrieves the size of the message at the specified offset for a given topic partition"),
    Command(this, "kgetminmax", getMessageMinMaxSize, (Seq("topic", "partition", "startOffset", "endOffset"), Seq("fetchSize")), help = "Retrieves the smallest and largest message sizes for a range of offsets for a given partition"),
    Command(this, "kimport", importMessages, (Seq("topic", "fileType", "filePath"), Seq.empty), "Imports messages into a new/existing topic"),
    Command(this, "kinbound", inboundMessages, (Seq.empty, Seq("prefix")), "Retrieves a list of topics with new messages (since last query)"),
    Command(this, "klast", getLastOffset, (Seq("topic", "partition"), Seq.empty), help = "Returns the last offset for a given topic"),
    Command(this, "kls", listTopics, (Seq.empty, Seq("prefix")), help = "Lists all existing topics"),
    Command(this, "kmk", createTopic, (Seq("topic", "partitions", "replicas"), Seq.empty), "Creates a new topic"),
    Command(this, "knext", nextMessage, (Seq.empty, Seq.empty), "Attempts to retrieve the next message"),
    Command(this, "koffset", getOffset, (Seq("topic", "partition"), Seq("time=YYYY-MM-DDTHH:MM:SS")), "Returns the offset at a specific instant-in-time for a given topic"),
    Command(this, "kpush", publishMessage, (Seq("topic", "key"), Seq.empty), "Publishes a message to a topic"),
    Command(this, "krm", deleteTopic, (Seq("topic"), Seq.empty), "Deletes a topic (DESTRUCTIVE)"),
    Command(this, "kstats", getStatistics, (Seq("topic"), Seq("beginPartition", "endPartition")), help = "Returns the partition details for a given topic"))

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
   * "kmk" - Creates a new topic
   * @example {{{ kmk com.shocktrade.alerts }}}
   */
  def createTopic(args: String*) {
    import _root_.kafka.admin.AdminUtils
    import org.I0Itec.zkclient.ZkClient

    val Seq(topic, partitions, replicas, _*) = args
    val topicConfig = new java.util.Properties()

    new ZkClient(rt.remoteHost) use (AdminUtils.createTopic(_, topic, parseInt("partitions", partitions), parseInt("replicas", replicas), topicConfig))
  }

  /**
   * "krm" - Deletes a new topic
   * @example {{{ krm com.shocktrade.alerts }}}
   */
  def deleteTopic(args: String*) {
    import _root_.kafka.admin.AdminUtils
    import org.I0Itec.zkclient.ZkClient

    val Seq(topic, _*) = args

    new ZkClient(rt.remoteHost) use (AdminUtils.deleteTopic(_, topic))
  }

  /**
   * "kdumpf" - Dumps the contents of a specific topic to a file
   */
  def exportToFile(args: String*): Long = {
    import java.io.{DataOutputStream, FileOutputStream}

    // get the arguments
    val Seq(file, name, partition, _*) = args
    val startOffset = extract(args, 3) map (parseLong("startOffset", _))
    val endOffset = extract(args, 4) map (parseLong("endOffset", _))
    val counts = extract(args, 5) map (_.toLowerCase) exists (_ == "-c")
    val blockSize = extract(args, 6) map (parseInt("blockSize", _))

    // output the output file
    var count = 0L
    new DataOutputStream(new FileOutputStream(file)) use { fos =>
      // perform the action
      new KafkaSubscriber(Topic(name, parseInt("partition", partition)), brokers, correlationId) use {
        _.consume(startOffset, endOffset, blockSize, listener = new MessageConsumer {
          override def consume(offset: Long, nextOffset: Option[Long], message: Array[Byte]) {
            if (counts) fos.writeInt(message.length)
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
   * @example {{{ kfetch com.shocktrade.alerts 0 devc  }}}
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
      case Some(fetchSize) => rt.defaultFetchSize = fetchSize.toInt
      case None => rt.defaultFetchSize
    }
  }

  /**
   * "kfirst" - Returns the first offset for a given topic
   */
  def getFirstOffset(args: String*): Option[Long] = {
    // get the arguments
    val Seq(name, partition, _*) = args

    // perform the action
    new KafkaSubscriber(Topic(name, parseInt("partition", partition)), brokers, correlationId) use (_.getFirstOffset)
  }

  /**
   * "kgeta" - Returns the key-value pairs of an Avro message from a Kafka partition
   * @example {{{ kavrofields avro/schema1.avsc com.shocktrade.alerts 0 58500700 }}}
   * @example {{{ kavrofields avro/schema2.avsc com.shocktrade.alerts 9 1799020 }}}
   */
  def getMessageAvro(args: String*)(implicit out: PrintStream): Seq[AvroRecord] = {
    import scala.collection.JavaConverters._

    // get the arguments
    val Seq(schemaPath, name, partition, _*) = args
    val offset = extract(args, 3) map (parseLong("offset", _))
    val blockSize = extract(args, 4) map (parseInt("blockSize", _))

    // get the decoder
    val decoder = getAvroDecoder(schemaPath)

    // perform the action
    var results: Seq[AvroRecord] = Nil
    new KafkaSubscriber(Topic(name, parseInt("partition", partition)), brokers, correlationId) use {
      _.consume(offset, offset map (_ + 1), blockSize, listener = new MessageConsumer {
        override def consume(offset: Long, nextOffset: Option[Long], message: Array[Byte]) {
          decoder.decode(message) match {
            case Success(record) =>
              cursor = nextOffset map (nextOffset => MessageCursor(name, partition.toInt, nextOffset, AVRO))
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
  def getMessage(args: String*)(implicit out: PrintStream): Array[Byte] = {
    // get the arguments
    val Seq(name, partition, offset, _*) = args
    val fetchSize = extract(args, 3) map (parseInt("fetchSize", _)) getOrElse rt.defaultFetchSize

    // perform the action
    var bytes: Array[Byte] = Array()
    new KafkaSubscriber(Topic(name, parseInt("partition", partition)), brokers, correlationId) use {
      _.fetch(offset.toLong, fetchSize).headOption map { m =>
        bytes = m.message
        cursor = Option(m.nextOffset) map (nextOffset => MessageCursor(name, partition.toInt, nextOffset, BINARY))
      }
    }
    bytes
  }

  case class MessageCursor(topic: String, partition: Int, offset: Long, encoding: MessageEncoding)

  /**
   * "kgetsize" - Returns the size of the message for a given topic partition and offset
   * @example {{{ kgetsize com.shocktrade.alerts 0 45913975 }}}
   */
  def getMessageSize(args: String*): Option[Int] = {
    // get the arguments
    val Seq(name, partition, offset, _*) = args
    val fetchSize = extract(args, 3) map (parseInt("fetchSize", _)) getOrElse rt.defaultFetchSize

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
    val fetchSize = extract(args, 4) map (parseInt("fetchSize", _)) getOrElse rt.defaultFetchSize

    // perform the action
    new KafkaSubscriber(Topic(name, parseInt("partition", partition)), brokers, correlationId) use { subscriber =>
      val offsets = startOffset.toLong to endOffset.toLong
      val messages = subscriber.fetch(offsets, fetchSize).map(_.message.length)
      if (messages.nonEmpty) Seq(MessageMaxMin(messages.min, messages.max)) else Seq.empty
    }
  }

  case class MessageMaxMin(minimumSize: Int, maximumSize: Int)

  /**
   * "klast" - Returns the last offset for a given topic
   */
  def getLastOffset(args: String*): Option[Long] = {
    // get the arguments
    val Seq(name, partition, _*) = args

    // perform the action
    new KafkaSubscriber(Topic(name, parseInt("partition", partition)), brokers, correlationId) use (_.getLastOffset)
  }

  /**
   * "knext" - Optionally returns the next message
   * @example {{{ knext }}}
   */
  def nextMessage(args: String*)(implicit out: PrintStream): Option[Any] = {
    cursor map { case MessageCursor(topic, partition, offset, encoding) =>
      encoding match {
        case BINARY =>
          getMessage(Seq(topic, partition.toString, offset.toString): _*)
        case AVRO =>
          getMessageAvro(Seq(topic, partition.toString, offset.toString): _*)
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
   * "kstats" - Returns the number of available messages for a given topic
   * @example {{{ kstats com.shocktrade.alerts 0 4 }}}
   */
  def getStatistics(args: String*): Iterable[TopicOffsets] = {
    // interpret based on the input arguments
    val results = args.toList match {
      case topic :: Nil =>
        val partitions = KafkaSubscriber.listTopics(zk, brokers, correlationId).filter(_.topic == topic).map(_.partitionId)
        if (partitions.nonEmpty) Some((topic, partitions.min, partitions.max)) else None

      case topic :: partition :: Nil =>
        val partitions = KafkaSubscriber.listTopics(zk, brokers, correlationId).filter(_.topic == topic).map(_.partitionId)
        if (partitions.nonEmpty) Some((topic, parseInt("partition", partition), partitions.max)) else None

      case topic :: partitionA :: partitionB :: Nil =>
        Some((topic, partitionA.toInt, partitionB.toInt))

      case _ =>
        None
    }

    results match {
      case Some((name, partition0, partition1)) =>
        // determine the difference between the first and last offsets
        for {
          partition <- partition0 to partition1
          first <- getFirstOffset(name, partition.toString)
          last <- getLastOffset(name, partition.toString)
        } yield TopicOffsets(name, partition, first, last, last - first)
      case _ => Seq.empty
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
   * @param prefix the given topic prefix (e.g. "myTopic123")
   * @return an iteration of inbound message statistics
   */
  private def inboundMessageStatistics(prefix: Option[String] = None): Iterable[Inbound] = {
    // start by retrieving a list of all topics
    val topics = KafkaSubscriber.listTopics(zk, brokers, correlationId)
      .filter(t => t.topic == prefix.getOrElse(t.topic))
      .groupBy(_.topic)

    // generate the inbound data
    val inboundData = (topics flatMap { case (topic, details) =>
      // get the range of partitions for each topic
      val partitions = details.map(_.partitionId)
      val (beginPartition, endPartition) = (partitions.min, partitions.max)

      // retrieve the statistics for each topic
      getStatistics(topic, beginPartition.toString, endPartition.toString) map { o =>
        val prevInbound = incomingMessageCache.get(Topic(o.name, o.partition))
        val lastCheckTime = prevInbound.map(_.lastCheckTime.getTime) getOrElse System.currentTimeMillis()
        val currentTime = System.currentTimeMillis()
        val elapsedTime = 1 + (currentTime - lastCheckTime) / 1000L
        val change = prevInbound map (o.endOffset - _.endOffset) getOrElse 0L
        val rate = BigDecimal(change.toDouble / elapsedTime).setScale(1, BigDecimal.RoundingMode.UP).toDouble
        Inbound(o.name, o.partition, o.startOffset, o.endOffset, change, rate, new Date(currentTime))
      }
    }).toSeq

    // cache the unfiltered inbound data
    incomingMessageCache = incomingMessageCache ++ Map(inboundData map (i => Topic(i.topic, i.partition) -> i): _*)

    // filter out the non-changed records
    inboundData.filterNot(_.change == 0) sortBy (-_.change)
  }

  case class Inbound(topic: String, partition: Int, startOffset: Long, endOffset: Long, change: Long, msgsPerSec: Double, lastCheckTime: Date)

  /**
   * "kbrokers" - Retrieves the list of Kafka brokers
   */
  def listBrokers(args: String*): Seq[BrokerDetails] = KafkaSubscriber.getBrokerList(zk)

  /**
   * "kls" - Lists all existing topicList
   */
  def listTopics(args: String*): Seq[TopicDetail] = {
    val prefix = args.headOption

    KafkaSubscriber.listTopics(zk, brokers, correlationId) flatMap { t =>
      val detail = TopicDetail(t.topic, t.partitionId, t.leader map (_.toString) getOrElse "N/A", t.replicas.size)
      if (prefix.isEmpty || prefix.exists(t.topic.startsWith)) Some(detail) else None
    }
  }

  /**
   * "kpush" - Returns the EOF offset for a given topic
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
   * kchka - Scans and verifies that a set of messages (specific offset range) can be read by the specified schema
   * @example {{{ kchka avro/schema1.avsc com.shocktrade.alerts 0 1000 2000 }}}
   */
  def scanTopicAvro(args: String*)(implicit out: PrintStream): Seq[AvroVerification] = {
    // get the arguments
    val Seq(schemaPath, name, partition, startOffset, endOffset, _*) = args
    val batchSize = extract(args, 5) map (parseInt("batchSize", _)) getOrElse 10
    val blockSize = extract(args, 6) map (parseInt("blockSize", _)) getOrElse 8192

    // get the decoder
    val decoder = getAvroDecoder(schemaPath)

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
          message.sliding(rt.columns, rt.columns) foreach { bytes =>
            out.println("[%04d] %-80s %-40s".format(offset, asHexString(bytes), asChars(bytes)))
            count += 1
          }
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
          message.sliding(rt.columns, rt.columns) foreach { bytes =>
            out.println("[%04d] %-80s %-40s".format(offset, asHexString(bytes), asChars(bytes)))
            count += 1
          }
        }
      })
    count
  }

  private def getAvroDecoder(schemaPath: String): AvroDecoder = {
    // ensure the file exists
    val schemaFile = new File(schemaPath)
    if (!schemaFile.exists()) {
      throw new IllegalStateException(s"Schema file '${schemaFile.getAbsolutePath}' not found")
    }

    // retrieve the schema as a string
    val schemaString = Source.fromFile(schemaFile).getLines() mkString "\n"
    new AvroDecoder(schemaString)
  }

  /**
   * Converts the given long value into a byte array
   * @param value the given long value
   * @return a byte array
   */
  private def toBytes(value: Long): Array[Byte] = allocate(8).putLong(value).array()

  case class AvroRecord(field: String, value: Any, `type`: String)

  case class AvroVerification(verified: Int, failed: Int)

  case class TopicDetail(name: String, partition: Int, leader: String, version: Int)

  case class TopicOffsets(name: String, partition: Int, startOffset: Long, endOffset: Long, messagesAvailable: Long)

}

object KafkaModule {

  case class MessageEncoding(value: Int) extends AnyVal

  val BINARY = MessageEncoding(0)
  val AVRO = MessageEncoding(1)

}
