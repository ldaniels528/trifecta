package com.ldaniels528.verify.modules.kafka

import java.io.{File, PrintStream}
import java.nio.ByteBuffer._
import java.text.SimpleDateFormat

import com.ldaniels528.tabular.Tabular
import com.ldaniels528.verify.io.Compression
import com.ldaniels528.verify.io.avro.{AvroDecoder, AvroTables}
import com.ldaniels528.verify.modules.Module
import com.ldaniels528.verify.modules.Module.Command
import com.ldaniels528.verify.modules.kafka.KafkaSubscriber.{BrokerDetails, MessageData}
import com.ldaniels528.verify.util.VerifyUtils._
import com.ldaniels528.verify.{CommandParser, VerifyShellRuntime}

import scala.collection.mutable
import scala.concurrent.duration._
import scala.io.Source
import scala.util.{Failure, Success, Try}

/**
 * Kafka Module
 * @author lawrence.daniels@gmail.com
 */
class KafkaModule(rt: VerifyShellRuntime, out: PrintStream)
  extends Module with Compression {
  // date parser instance
  private val sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss")

  // define a custom tabular instance
  private val tabular = new Tabular() with AvroTables

  // create the ZooKeeper proxy
  private val zk = rt.zkProxy

  // get the list of brokers from zookeeper
  private val brokers: Seq[Broker] = KafkaSubscriber.getBrokerList(zk) map (b => Broker(b.host, b.port))

  // set the default correlation ID
  private var correlationId: Int = (Math.random * Int.MaxValue).toInt
  private var columns = 25

  // the name of the module
  val name = "kafka"

  // the bound commands
  val getCommands = Seq(
    Command(this, "kbrokers", listBrokers, (Seq.empty, Seq.empty), help = "Returns a list of the registered brokers from ZooKeeper"),
    Command(this, "kchka", verifyTopicAvro, (Seq("schemaPath", "topic", "partition", "startOffset", "endOffset"), Seq("batchSize", "blockSize")), help = "Verifies that a set of messages (specific offset range) can be read by the specified schema"),
    Command(this, "kcolumns", messageColumnsGetOrSet, (Seq.empty, Seq("columns")), help = "Retrieves or sets the column width for message output"),
    Command(this, "kcommit", commitOffset, (Seq("topic", "partition", "groupId", "offset"), Seq("metadata")), "Commits the offset for a given topic and group"),
    Command(this, "kcount", countMessages, (Seq("topic", "partition"), Seq.empty), help = "Returns the number of messages available for a given topic"),
    Command(this, "kdump", dumpBinary, (Seq("topic", "partition"), Seq("startOffset", "endOffset", "blockSize")), "Dumps the contents of a specific topic [as binary] to the console"),
    Command(this, "kdumpa", dumpAvro, (Seq("schemaPath", "topic", "partition"), Seq("startOffset", "endOffset", "blockSize", "fields")), "Dumps the contents of a specific topic [as Avro] to the console"),
    Command(this, "kdumpr", dumpRaw, (Seq("topic", "partition"), Seq("startOffset", "endOffset", "blockSize")), "Dumps the contents of a specific topic [as raw ASCII] to the console"),
    Command(this, "kdumpf", dumpToFile, (Seq("file", "topic", "partition"), Seq("startOffset", "endOffset", "flags", "blockSize")), "Dumps the contents of a specific topic to a file"),
    Command(this, "kfetch", fetchOffsets, (Seq("topic", "partition", "groupId"), Seq.empty), "Retrieves the offset for a given topic and group"),
    Command(this, "kfetchsize", fetchSizeGetOrSet, (Seq.empty, Seq("fetchSize")), help = "Retrieves or sets the default fetch size for all Kafka queries"),
    Command(this, "kfirst", getFirstOffset, (Seq("topic", "partition"), Seq.empty), help = "Returns the first offset for a given topic"),
    Command(this, "kget", getMessage, (Seq("topic", "partition", "offset"), Seq("fetchSize")), help = "Retrieves the message at the specified offset for a given topic partition"),
    Command(this, "kgeta", getMessageAvro, (Seq("schemaPath", "topic", "partition"), Seq("offset", "blockSize")), help = "Returns the key-value pairs of an Avro message from a topic partition"),
    Command(this, "kgetsize", getMessageSize, (Seq("topic", "partition", "offset"), Seq("fetchSize")), help = "Retrieves the size of the message at the specified offset for a given topic partition"),
    Command(this, "kgetmaxsize", getMessageMaxSize, (Seq("topic", "partition", "startOffset", "endOffset"), Seq("fetchSize")), help = "Retrieves the size of the largest message for the specified range of offsets for a given topic partition"),
    Command(this, "kgetminsize", getMessageMinSize, (Seq("topic", "partition", "startOffset", "endOffset"), Seq("fetchSize")), help = "Retrieves the size of the smallest message for the specified range of offsets for a given topic partition"),
    Command(this, "kimport", importMessages, (Seq("topic", "fileType", "filePath"), Seq.empty), "Imports messages into a new/existing topic"),
    Command(this, "klast", getLastOffset, (Seq("topic", "partition"), Seq.empty), help = "Returns the last offset for a given topic"),
    Command(this, "kls", listTopics, (Seq.empty, Seq("prefix")), help = "Lists all existing topics"),
    Command(this, "kmk", createTopic, (Seq("topic", "partitions", "replicas"), Seq.empty), "Returns the system time as an EPOC in milliseconds"),
    Command(this, "koffset", getOffset, (Seq("topic", "partition"), Seq("time=YYYY-MM-DDTHH:MM:SS")), "Returns the offset at a specific instant-in-time for a given topic"),
    Command(this, "kpush", publishMessage, (Seq("topic", "key"), Seq.empty), "Publishes a message to a topic"),
    Command(this, "krm", deleteTopic, (Seq("topic"), Seq.empty), "Deletes a topic"),
    Command(this, "kstats", getStatistics, (Seq("topic", "beginPartition", "endPartition"), Seq.empty), help = "Returns the parition details for a given topic"))

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
    new KafkaSubscriber(Topic(name, partition.toInt), brokers, correlationId) use (_.commitOffsets(groupId, offset.toLong, metadata))
  }

  /**
   * "kcount" - Returns the number of available messages for a given topic
   */
  def countMessages(args: String*): Option[Long] = {
    // get the arguments
    val Seq(name, partition, _*) = args

    // determine the difference between the first and last offsets
    for {
      first <- getFirstOffset(name, partition)
      last <- getLastOffset(name, partition)
    } yield last - first
  }

  /**
   * "kmk" - Creates a new topic
   */
  def createTopic(args: String*) {
    import _root_.kafka.admin.AdminUtils
    import org.I0Itec.zkclient.ZkClient

    val Seq(topic, partitions, replicas, _*) = args
    val topicConfig = new java.util.Properties()

    new ZkClient(rt.remoteHost) use (AdminUtils.createTopic(_, topic, partitions.toInt, replicas.toInt, topicConfig))
  }

  /**
   * "krm" - Deletes a new topic
   */
  def deleteTopic(args: String*) {
    import _root_.kafka.admin.AdminUtils
    import org.I0Itec.zkclient.ZkClient

    val Seq(topic, _*) = args

    new ZkClient(rt.remoteHost) use (AdminUtils.deleteTopic(_, topic))
  }

  /**
   * "kdumpa" - Dumps the contents of a specific topic to the console [as AVRO]
   * @example {{{ kdumpa avro/schema1.avsc com.shocktrade.alerts 0 58500700 58500724 }}}
   * @example {{{ kdumpa avro/schema2.avsc com.shocktrade.alerts 9 1799020 1799029 1024 field1+field2+field3+field4 }}}
   */
  def dumpAvro(args: String*): Long = {
    import org.apache.avro.generic.GenericRecord

    // get the arguments
    val Seq(schemaPath, name, partition, _*) = args
    val startOffset = extract(args, 3) map (_.toLong)
    val endOffset = extract(args, 4) map (_.toLong)
    val blockSize = extract(args, 5) map (_.toInt)
    val fields: Seq[String] = extract(args, 6) map (_.split("[+]")) map (_.toSeq) getOrElse Seq.empty

    // get the decoder
    val decoder = getAvroDecoder(schemaPath)
    val records = mutable.Buffer[GenericRecord]()

    // perform the action
    val offset = new KafkaSubscriber(Topic(name, partition.toInt), brokers, correlationId) use {
      _.consume(startOffset, endOffset, blockSize, new MessageConsumer {
        override def consume(offset: Long, message: Array[Byte]) {
          decoder.decode(message) match {
            case Success(record) =>
              records += record
              ()
            case Failure(e) =>
              out.println("[%04d] %s".format(offset, e.getMessage))
          }
        }
      })
    }

    // transform the records into a table
    tabular.transformAvro(records, fields) foreach out.println

    offset
  }

  /**
   * "kdump" - Dumps the contents of a specific topic to the console [as binary]
   * @example {{{ kdump com.shocktrade.alerts 0 58500700 58500724 }}}
   */
  def dumpBinary(args: String*): Long = {
    // convert the tokens into a parameter list
    val params = CommandParser.parseArgs(args)

    // get the arguments
    val Seq(name, partition, _*) = args
    val startOffset = extract(args, 2) map (_.toLong)
    val endOffset = extract(args, 3) map (_.toLong)
    val blockSize = extract(args, 4) map (_.toInt)
    //val outputFile = params.

    // perform the action
    new KafkaSubscriber(Topic(name, partition.toInt), brokers, correlationId) use {
      _.consume(startOffset, endOffset, blockSize, new MessageConsumer {
        override def consume(offset: Long, message: Array[Byte]) {
          message.sliding(40, 40) foreach { bytes =>
            out.println("[%04d] %-80s %-40s".format(offset, asHexString(bytes), asChars(bytes)))
          }
        }
      })
    }
  }

  /**
   * "kdumpr" - Dumps the contents of a specific topic to the console [as raw ASCII]
   */
  def dumpRaw(args: String*): Long = {
    // get the arguments
    val Seq(name, partition, _*) = args
    val startOffset = extract(args, 2) map (_.toLong)
    val endOffset = extract(args, 3) map (_.toLong)
    val blockSize = extract(args, 4) map (_.toInt)

    // perform the action
    new KafkaSubscriber(Topic(name, partition.toInt), brokers, correlationId) use {
      _.consume(startOffset, endOffset, blockSize, new MessageConsumer {
        override def consume(offset: Long, message: Array[Byte]) {
          out.println("[%04d] %s".format(offset, new String(deflate(message), "UTF-8")))
        }
      })
    }
  }

  /**
   * "kdumpf" - Dumps the contents of a specific topic to a file
   */
  def dumpToFile(args: String*): Long = {
    import java.io.{DataOutputStream, FileOutputStream}

    // get the arguments
    val Seq(file, name, partition, _*) = args
    val startOffset = extract(args, 3) map (_.toLong)
    val endOffset = extract(args, 4) map (_.toLong)
    val counts = extract(args, 5) map (_.toLowerCase) exists (_ == "-c")
    val blockSize = extract(args, 6) map (_.toInt)

    // output the output file
    var count = 0L
    new DataOutputStream(new FileOutputStream(file)) use { fos =>
      // perform the action
      new KafkaSubscriber(Topic(name, partition.toInt), brokers, correlationId) use {
        _.consume(startOffset, endOffset, blockSize, listener = new MessageConsumer {
          override def consume(offset: Long, message: Array[Byte]) {
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
    new KafkaSubscriber(Topic(name, partition.toInt), brokers, correlationId) use (_.fetchOffsets(groupId))
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
   * "kfind" - Returns the message for a given topic partition by its message ID
   */
  def findMessage(args: String*): Option[MessageData] = {
    // get the arguments
    val Seq(name, partition, messageID, _*) = args
    val fetchSize = extract(args, 3) map (_.toInt) getOrElse 8192

    // perform the action
    new KafkaSubscriber(Topic(name, partition.toInt), brokers, correlationId) use { subscriber =>
      // get the start and end offsets for the topic partition
      val startOffset = subscriber.getFirstOffset getOrElse (throw new IllegalStateException("Could not determine start of partition"))
      val endOffset = subscriber.getLastOffset getOrElse (throw new IllegalStateException("Could not determine end of partition"))
      findMessage(subscriber, startOffset, endOffset, fetchSize)
    }
  }

  private def findMessage(subscriber: KafkaSubscriber, startOffset: Long, endOffset: Long, fetchSize: Int): Option[MessageData] = {
    // search for the message key
    (startOffset to endOffset).sliding(10, 10) foreach { offsets =>
      subscriber.fetch(offsets, fetchSize) foreach { m =>

      }
    }
    None
  }

  /**
   * "kfirst" - Returns the first offset for a given topic
   */
  def getFirstOffset(args: String*): Option[Long] = {
    // get the arguments
    val Seq(name, partition, _*) = args

    // perform the action
    new KafkaSubscriber(Topic(name, partition.toInt), brokers, correlationId) use (_.getFirstOffset)
  }

  /**
   * "klast" - Returns the last offset for a given topic
   */
  def getLastOffset(args: String*): Option[Long] = {
    // get the arguments
    val Seq(name, partition, _*) = args

    // perform the action
    new KafkaSubscriber(Topic(name, partition.toInt), brokers, correlationId) use (_.getLastOffset)
  }

  /**
   * "kgeta" - Returns the key-value pairs of an Avro message from a Kafka partition
   * @example {{{ kavrofields avro/schema1.avsc com.shocktrade.alerts 0 58500700 }}}
   * @example {{{ kavrofields avro/schema2.avsc com.shocktrade.alerts 9 1799020 }}}
   */
  def getMessageAvro(args: String*): Seq[AvroRecord] = {
    import scala.collection.JavaConverters._

    // get the arguments
    val Seq(schemaPath, name, partition, _*) = args
    val offset = extract(args, 3) map (_.toLong)
    val blockSize = extract(args, 4) map (_.toInt)

    // get the decoder
    val decoder = getAvroDecoder(schemaPath)

    // perform the action
    var results: Seq[AvroRecord] = Nil
    new KafkaSubscriber(Topic(name, partition.toInt), brokers, correlationId) use {
      _.consume(offset, offset map (_ + 1), blockSize, listener = new MessageConsumer {
        override def consume(offset: Long, message: Array[Byte]) {
          decoder.decode(message) match {
            case Success(record) =>
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
  def getMessage(args: String*): Option[Int] = {
    // get the arguments
    val Seq(name, partition, offset, _*) = args
    val fetchSize = extract(args, 3) map (_.toInt) getOrElse rt.defaultFetchSize

    // perform the action
    val width1 = columns * 3
    val width2 = columns * 2
    new KafkaSubscriber(Topic(name, partition.toInt), brokers, correlationId) use {
      _.fetch(offset.toLong, fetchSize).headOption map { m =>
        var index = 0
        val length1 = 1 + Math.log10(m.offset).toInt
        val length2 = 1 + Math.log10(m.message.length).toInt
        val myFormat = s"[%0${length1}d:%0${length2}d] %-${width1}s %-${width2}s"
        m.message.sliding(columns, columns) foreach { bytes =>
          out.println(myFormat.format(m.offset, index, asHexString(bytes), asChars(bytes)))
          index += columns
        }
        m.message.length
      }
    }
  }

  /**
   * "kgetsize" - Returns the size of the message for a given topic partition and offset
   * @example {{{ kgetsize com.shocktrade.alerts 0 45913975 }}}
   */
  def getMessageSize(args: String*): Option[Int] = {
    // get the arguments
    val Seq(name, partition, offset, _*) = args
    val fetchSize = extract(args, 3) map (_.toInt) getOrElse rt.defaultFetchSize

    // perform the action
    new KafkaSubscriber(Topic(name, partition.toInt), brokers, correlationId) use {
      _.fetch(offset.toLong, fetchSize).headOption map (_.message.length)
    }
  }

  /**
   * "kgetmaxsize" - Returns the largest message size for a given topic partition and offset range
   * @example {{{ kgetmaxsize com.shocktrade.alerts 0 45913900 45913975 }}}
   */
  def getMessageMaxSize(args: String*): Int = {
    // get the arguments
    val Seq(name, partition, startOffset, endOffset, _*) = args
    val fetchSize = extract(args, 4) map (_.toInt) getOrElse rt.defaultFetchSize

    // perform the action
    new KafkaSubscriber(Topic(name, partition.toInt), brokers, correlationId) use {
      val offsets = startOffset.toLong to endOffset.toLong
      _.fetch(offsets, fetchSize).map(_.message.length).max
    }
  }

  /**
   * "kgetminsize" - Returns the smallest message size for a given topic partition and offset range
   * @example {{{ kgetminsize com.shocktrade.alerts 0 45913900 45913975 }}}
   */
  def getMessageMinSize(args: String*): Int = {
    // get the arguments
    val Seq(name, partition, startOffset, endOffset, _*) = args
    val fetchSize = extract(args, 4) map (_.toInt) getOrElse rt.defaultFetchSize

    // perform the action
    new KafkaSubscriber(Topic(name, partition.toInt), brokers, correlationId) use {
      val offsets = startOffset.toLong to endOffset.toLong
      _.fetch(offsets, fetchSize).map(_.message.length).min
    }
  }

  /**
   * "koffset" - Returns the offset at a specific instant-in-time for a given topic
   * Example: koffset flights 0 2014-05-14T14:30:11
   */
  def getOffset(args: String*): Option[Long] = {
    // get the arguments
    val Seq(name, partition, _*) = args
    val sysTimeMillis = extract(args, 2) map (sdf.parse(_).getTime) getOrElse -1L

    // perform the action
    new KafkaSubscriber(Topic(name, partition.toInt), brokers, correlationId) use (_.getOffsetsBefore(sysTimeMillis))
  }

  /**
   * "kstats" - Returns the number of available messages for a given topic
   */
  def getStatistics(args: String*): Seq[TopicOffsets] = {
    // get the arguments
    val Seq(name, beginPartition, endPartition, _*) = args

    // determine the difference between the first and last offsets
    for {
      partition <- beginPartition.toInt to endPartition.toInt
      first <- getFirstOffset(name, partition.toString)
      last <- getLastOffset(name, partition.toString)
    } yield TopicOffsets(name, partition, first, last, last - first)
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
   * "kcolumns" - Retrieves or sets the column width for message output
   * @example {{{ kcolumns 30 }}}
   */
  def messageColumnsGetOrSet(args: String*): Any = {
    args.headOption match {
      case Some(arg) => columns = arg.toInt
      case None => columns
    }
  }

  /**
   * "kpush" - Returns the EOF offset for a given topic
   */
  def publishMessage(args: String*): Unit = {
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
   * kchka - Verifies that a set of messages (specific offset range) can be read by the specified schema
   * @example {{{ kchka avro/schema1.avsc com.shocktrade.alerts 0 1000 2000 }}}
   */
  def verifyTopicAvro(args: String*): Seq[AvroVerification] = {
    // get the arguments
    val Seq(schemaPath, name, partition, startOffset, endOffset, _*) = args
    val batchSize = extract(args, 5) map (_.toInt) getOrElse 10
    val blockSize = extract(args, 6) map (_.toInt) getOrElse 8192

    // get the decoder
    val decoder = getAvroDecoder(schemaPath)

    // check all records within the range
    var verified = 0
    var errors = 0
    new KafkaSubscriber(Topic(name, partition.toInt), brokers, correlationId) use { subscriber =>
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
  def watchTopic(args: String*): Long = {
    // get the arguments
    val Seq(name, partition, _*) = args
    val duration = (extract(args, 2) map (_.toInt) getOrElse 60).seconds

    // perform the action
    var count = 0L
    KafkaSubscriber.watch(Topic(name, partition.toInt), brokers, None, duration, correlationId,
      new MessageConsumer {
        override def consume(offset: Long, message: Array[Byte]) {
          message.sliding(40, 40) foreach { bytes =>
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
  def watchTopicWithConsumerGroup(args: String*): Long = {
    // get the arguments
    val Seq(name, partition, groupId, _*) = args
    val duration = (extract(args, 3) map (_.toInt) getOrElse 60).seconds

    // perform the action
    var count = 0L
    KafkaSubscriber.watchGroup(Topic(name, partition.toInt), brokers, groupId, duration, correlationId,
      new MessageConsumer {
        override def consume(offset: Long, message: Array[Byte]) {
          message.sliding(40, 40) foreach { bytes =>
            out.println("[%04d] %-80s %-40s".format(offset, asHexString(bytes), asChars(bytes)))
            count += 1
          }
        }
      })
    count
  }

  /**
   * Returns the ASCII array as a character string
   * @param bytes the byte array
   * @return a character string representing the given byte array
   */
  private def asChars(bytes: Array[Byte]): String = {
    String.valueOf(bytes map (b => if (b >= 32 && b <= 126) b.toChar else '.'))
  }

  /**
   * Returns the byte array as a hex string
   * @param bytes the byte array
   * @return a hex string representing the given byte array
   */
  private def asHexString(bytes: Array[Byte]): String = {
    bytes map ("%02x".format(_)) mkString "."
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
