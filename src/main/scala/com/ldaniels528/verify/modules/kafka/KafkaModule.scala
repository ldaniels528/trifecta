package com.ldaniels528.verify.modules.kafka

import java.io.{DataOutputStream, File, FileOutputStream, PrintStream}
import java.nio.ByteBuffer._
import java.text.SimpleDateFormat

import com.ldaniels528.tabular.Tabular
import com.ldaniels528.verify.io.Compression
import com.ldaniels528.verify.io.avro.{AvroDecoder, AvroTables}
import com.ldaniels528.verify.modules.Module
import com.ldaniels528.verify.modules.Module.Command
import com.ldaniels528.verify.modules.kafka.KafkaSubscriber.MessageData
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

  // the name of the module
  val name = "kafka"

  // the bound commands
  val getCommands = Seq(
    Command(this, "kchka", topicAvroVerify, (Seq("schemaPath", "topic", "partition", "startOffset", "endOffset"), Seq("batchSize", "blockSize")), help = "Verifies that a set of messages (specific offset range) can be read by the specified schema"),
    Command(this, "kbrokers", topicBrokers, (Seq.empty, Seq.empty), help = "Returns a list of the registered brokers from ZooKeeper"),
    Command(this, "kcommit", topicCommit, (Seq("topic", "partition", "groupId", "offset"), Seq("metadata")), "Commits the offset for a given topic and group"),
    Command(this, "kcount", topicCount, (Seq("topic", "partition"), Seq.empty), help = "Returns the number of messages available for a given topic"),
    Command(this, "kdump", topicDumpBinary, (Seq("topic", "partition"), Seq("startOffset", "endOffset", "blockSize")), "Dumps the contents of a specific topic [as binary] to the console"),
    Command(this, "kdumpa", topicDumpAvro, (Seq("schemaPath", "topic", "partition"), Seq("startOffset", "endOffset", "blockSize", "fields")), "Dumps the contents of a specific topic [as Avro] to the console"),
    Command(this, "kdumpr", topicDumpRaw, (Seq("topic", "partition"), Seq("startOffset", "endOffset", "blockSize")), "Dumps the contents of a specific topic [as raw ASCII] to the console"),
    Command(this, "kdumpf", topicDumpToFile, (Seq("file", "topic", "partition"), Seq("startOffset", "endOffset", "flags", "blockSize")), "Dumps the contents of a specific topic to a file"),
    Command(this, "kfetch", topicFetchOffsets, (Seq("topic", "partition", "groupId"), Seq.empty), "Retrieves the offset for a given topic and group"),
    Command(this, "kfetchsize", topicFetchSize, (Seq.empty, Seq("fetchSize")), help = "Retrieves or sets the default fetch size for all Kafka queries"),
    Command(this, "kfirst", topicFirstOffset, (Seq("topic", "partition"), Seq.empty), help = "Returns the first offset for a given topic"),
    Command(this, "kget", topicGetMessage, (Seq("topic", "partition", "offset"), Seq("fetchSize")), help = "Retrieves the message at the specified offset for a given topic partition"),
    Command(this, "kgeta", topicGetAvro, (Seq("schemaPath", "topic", "partition"), Seq("offset", "blockSize")), help = "Returns the key-value pairs of an Avro message from a topic partition"),
    Command(this, "kgetsize", topicGetMessageSize, (Seq("topic", "partition", "offset"), Seq("fetchSize")), help = "Retrieves the size of the message at the specified offset for a given topic partition"),
    Command(this, "kgetmaxsize", topicGetMaxMessageSize, (Seq("topic", "partition", "startOffset", "endOffset"), Seq("fetchSize")), help = "Retrieves the size of the largest message for the specified range of offsets for a given topic partition"),
    Command(this, "kgetminsize", topicGetMinMessageSize, (Seq("topic", "partition", "startOffset", "endOffset"), Seq("fetchSize")), help = "Retrieves the size of the smallest message for the specified range of offsets for a given topic partition"),
    Command(this, "kimport", topicImport, (Seq("topic", "fileType", "filePath"), Seq.empty), "Imports data into a new/existing topic"),
    Command(this, "klast", topicLastOffset, (Seq("topic", "partition"), Seq.empty), help = "Returns the last offset for a given topic"),
    Command(this, "kls", topicList, (Seq.empty, Seq("prefix")), help = "Lists all existing topics"),
    Command(this, "kmk", topicMake, (Seq("topic", "partitions", "replicas"), Seq.empty), "Returns the system time as an EPOC in milliseconds"),
    Command(this, "koffset", topicOffset, (Seq("topic", "partition"), Seq("time=YYYY-MM-DDTHH:MM:SS")), "Returns the offset at a specific instant-in-time for a given topic"),
    Command(this, "kpush", topicPublish, (Seq("topic", "key"), Seq.empty), "Publishes a message to a topic"),
    Command(this, "krm", topicDelete, (Seq("topic"), Seq.empty), "Deletes a topic"),
    Command(this, "kstats", topicStats, (Seq("topic", "beginPartition", "endPartition"), Seq.empty), help = "Returns the parition details for a given topic"))

  override def shutdown() = ()

  /**
   * "kbrokers" - Retrieves the list of Kafka brokers
   */
  def topicBrokers(args: String*) = KafkaSubscriber.getBrokerList(zk)

  /**
   * "kcommit" - Commits the offset for a given topic and group ID
   */
  def topicCommit(args: String*): Option[Short] = {
    // get the arguments
    val Seq(name, partition, groupId, offset, _*) = args
    val metadata = extract(args, 4) getOrElse ""

    // perform the action
    new KafkaSubscriber(Topic(name, partition.toInt), brokers, correlationId) use (_.commitOffsets(groupId, offset.toLong, metadata))
  }

  /**
   * "kcount" - Returns the number of available messages for a given topic
   */
  def topicCount(args: String*): Option[Long] = {
    // get the arguments
    val Seq(name, partition, _*) = args

    // determine the difference between the first and last offsets
    for {
      first <- topicFirstOffset(name, partition)
      last <- topicLastOffset(name, partition)
    } yield last - first
  }

  /**
   * "kfetchsize" - Retrieves or sets the default fetch size for all Kafka queries
   * @param args the given command line arguments
   * @return
   */
  def topicFetchSize(args: String*) = {
    args.headOption match {
      case Some(fetchSize) => rt.defaultFetchSize = fetchSize.toInt
      case None => rt.defaultFetchSize
    }
  }

  /**
   * "krm" - Deletes a new topic
   */
  def topicDelete(args: String*) {
    import _root_.kafka.admin.AdminUtils
    import org.I0Itec.zkclient.ZkClient

    val Seq(topic, _*) = args

    new ZkClient(rt.remoteHost) use (AdminUtils.deleteTopic(_, topic))
  }

  /**
   * "kdump" - Dumps the contents of a specific topic to the console [as binary]
   * Example: kdump topics.ldaniels528.test1 0 58500700 58500724
   */
  def topicDumpBinary(args: String*): Long = {
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
   * "kmk" - Creates a new topic
   */
  def topicMake(args: String*) {
    import _root_.kafka.admin.AdminUtils
    import org.I0Itec.zkclient.ZkClient

    val Seq(topic, partitions, replicas, _*) = args
    val topicConfig = new java.util.Properties()

    new ZkClient(rt.remoteHost) use (AdminUtils.createTopic(_, topic, partitions.toInt, replicas.toInt, topicConfig))
  }

  /**
   * kchka - Verifies that a set of messages (specific offset range) can be read by the specified schema
   * Example: kchka avro/schema1.avsc topics.ldaniels528.test1 0 1000 2000
   */
  def topicAvroVerify(args: String*): Seq[AvroVerification] = {
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
   * "kdumpa" - Dumps the contents of a specific topic to the console [as AVRO]
   * Example1: kdumpa avro/schema1.avsc topics.ldaniels528.test1 0 58500700 58500724
   * Example2: kdumpa avro/schema2.avsc topics.ldaniels528.test2 9 1799020 1799029 1024 field1+field2+field3+field4
   */
  def topicDumpAvro(args: String*): Long = {
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
   * "kdumpr" - Dumps the contents of a specific topic to the console [as raw ASCII]
   */
  def topicDumpRaw(args: String*): Long = {
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
  def topicDumpToFile(args: String*): Long = {
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

  def topicFindMessage(subscriber: KafkaSubscriber, startOffset: Long, endOffset: Long, fetchSize: Int): Option[MessageData] = {
    // search for the message key
    (startOffset to endOffset).sliding(10, 10) foreach { offsets =>
      subscriber.fetch(offsets, fetchSize) foreach { m =>

      }
    }
    None
  }

  def topicImport(args: String*) = {
    // get the arguments
    val Seq(topic, fileType, rawFilePath, _*) = args

    // expand the file path
    val filePath = expandPath(rawFilePath)

    KafkaPublisher(brokers) use { publisher =>
      publisher.open()

      // process based on file type
      fileType.toLowerCase match {
        // import text file
        case "text" =>
          topicImportTextFile(publisher, topic, filePath)
        case "avro" =>
          topicImportAvroFile(publisher, filePath)
        case unknown =>
          throw new IllegalArgumentException(s"Unrecognized file type '$unknown'")
      }
    }
  }

  private def topicImportTextFile(publisher: KafkaPublisher, topic: String, filePath: String) {
    import scala.io.Source

    Source.fromFile(filePath).getLines() foreach { message =>
      publisher.publish(topic, toBytes(System.currentTimeMillis()), message.getBytes(rt.encoding))
    }
  }

  private def topicImportAvroFile(publisher: KafkaPublisher, filePath: String) {
    import org.apache.avro.file.DataFileReader
    import org.apache.avro.generic.{GenericDatumReader, GenericRecord}

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
        }
      }
    }
  }

  /**
   * "kls" - Lists all existing topicList
   */
  def topicList(args: String*): Seq[TopicDetail] = {
    val prefix = args.headOption

    KafkaSubscriber.listTopics(zk, brokers, correlationId) flatMap { t =>
      val detail = TopicDetail(t.topic, t.partitionId, t.leader map (_.toString) getOrElse "N/A", t.replicas.size)
      if (prefix.isEmpty || prefix.exists(t.topic.startsWith)) Some(detail) else None
    }
  }

  /**
   * "kfetch" - Returns the offsets for a given topic and group ID
   */
  def topicFetchOffsets(args: String*): Option[Long] = {
    // get the arguments
    val Seq(name, partition, groupId, _*) = args

    // perform the action
    new KafkaSubscriber(Topic(name, partition.toInt), brokers, correlationId) use (_.fetchOffsets(groupId))
  }

  /**
   * "kfind" - Returns the message for a given topic partition by its message ID
   */
  def topicFindMessage(args: String*) = {
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
   * "kgeta" - Returns the key-value pairs of an Avro message from a Kafka partition
   * Example1: kavrofields avro/schema1.avsc topics.ldaniels528.test1 0 58500700
   * Example2: kavrofields avro/schema2.avsc topics.ldaniels528.test2 9 1799020
   */
  def topicGetAvro(args: String*): Seq[AvroData] = {
    import scala.collection.JavaConverters._

    // get the arguments
    val Seq(schemaPath, name, partition, _*) = args
    val offset = extract(args, 3) map (_.toLong)
    val blockSize = extract(args, 4) map (_.toInt)

    // get the decoder
    val decoder = getAvroDecoder(schemaPath)

    // perform the action
    var results: Seq[AvroData] = Nil
    new KafkaSubscriber(Topic(name, partition.toInt), brokers, correlationId) use {
      _.consume(offset, offset map (_ + 1), blockSize, listener = new MessageConsumer {
        override def consume(offset: Long, message: Array[Byte]) {
          decoder.decode(message) match {
            case Success(record) =>
              val fields = record.getSchema.getFields.asScala.map(_.name.trim).toSeq
              results = fields map { f =>
                val v = record.get(f)
                AvroData(f, v, Option(v) map (_.getClass.getSimpleName) getOrElse "")
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
   */
  def topicGetMessage(args: String*) {
    // get the arguments
    val Seq(name, partition, offset, _*) = args
    val fetchSize = extract(args, 3) map (_.toInt) getOrElse rt.defaultFetchSize

    // perform the action
    new KafkaSubscriber(Topic(name, partition.toInt), brokers, correlationId) use {
      _.fetch(offset.toLong, fetchSize).headOption foreach { m =>
        m.message.sliding(40, 40) foreach { bytes =>
          out.println("[%04d] %-80s %-40s".format(m.offset, asHexString(bytes), asChars(bytes)))
        }
      }
    }
  }

  /**
   * "kgetsize" - Returns the size of the message for a given topic partition and offset
   */
  def topicGetMessageSize(args: String*): Option[Int] = {
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
   */
  def topicGetMaxMessageSize(args: String*): Int = {
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
   */
  def topicGetMinMessageSize(args: String*): Int = {
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
   * "kfirst" - Returns the first offset for a given topic
   */
  def topicFirstOffset(args: String*): Option[Long] = {
    // get the arguments
    val Seq(name, partition, _*) = args

    // perform the action
    new KafkaSubscriber(Topic(name, partition.toInt), brokers, correlationId) use (_.getFirstOffset)
  }

  /**
   * "klast" - Returns the last offset for a given topic
   */
  def topicLastOffset(args: String*): Option[Long] = {
    // get the arguments
    val Seq(name, partition, _*) = args

    // perform the action
    new KafkaSubscriber(Topic(name, partition.toInt), brokers, correlationId) use (_.getLastOffset)
  }

  /**
   * "koffset" - Returns the offset at a specific instant-in-time for a given topic
   * Example: koffset flights 0 2014-05-14T14:30:11
   */
  def topicOffset(args: String*): Option[Long] = {
    // get the arguments
    val Seq(name, partition, _*) = args
    val sysTimeMillis = extract(args, 2) map (sdf.parse(_).getTime) getOrElse -1L

    // perform the action
    new KafkaSubscriber(Topic(name, partition.toInt), brokers, correlationId) use (_.getOffsetsBefore(sysTimeMillis))
  }

  /**
   * "kpush" - Returns the EOF offset for a given topic
   */
  def topicPublish(args: String*): Unit = {
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
   * "kstats" - Returns the number of available messages for a given topic
   */
  def topicStats(args: String*): Seq[TopicOffsets] = {
    // get the arguments
    val Seq(name, beginPartition, endPartition, _*) = args

    // determine the difference between the first and last offsets
    for {
      partition <- beginPartition.toInt to endPartition.toInt
      first <- topicFirstOffset(name, partition.toString)
      last <- topicLastOffset(name, partition.toString)
    } yield TopicOffsets(name, partition, first, last, last - first)
  }

  /**
   * "kwatch" - Subscribes to a specific topic
   */
  def topicWatch(args: String*): Long = {
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
   * "kwatchgroup" - Subscribes to a specific topic
   */
  def topicWatchGroup(args: String*): Long = {
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

  private def asChars(bytes: Array[Byte]): String = {
    String.valueOf(bytes map (b => if (b >= 32 && b <= 126) b.toChar else '.'))
  }

  /**
   * Converts the given long value into a byte array
   * @param value the given long value
   * @return a byte array
   */
  private def toBytes(value: Long): Array[Byte] = allocate(8).putLong(value).array()

  case class AvroData(field: String, value: Any, `type`: String)
  
  case class AvroVerification(verified: Int, failed: Int)

  case class TopicDetail(name: String, partition: Int, leader: String, version: Int)

  case class TopicOffsets(name: String, partition: Int, startOffset: Long, endOffset: Long, messagesAvailable: Long)

}
