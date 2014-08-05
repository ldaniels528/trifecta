package com.ldaniels528.verify

import VerifyShell._
import com.ldaniels528.verify.io._
import com.ldaniels528.verify.io.avro._
import com.ldaniels528.verify.subsystems.zookeeper._
import com.ldaniels528.verify.subsystems.kafka._
import com.ldaniels528.verify.util.VerifyUtils._
import com.ldaniels528.verify.util.Tabular
import java.io.DataOutputStream

/**
 * Verify Console Shell Application
 * @author lawrence.daniels@gmail.com
 */
class VerifyShell(remoteHost: String, rt: VerifyShellRuntime) extends HttpResource with Compression {
  import java.io.{ ByteArrayOutputStream, FileOutputStream, PrintStream }
  import java.nio.ByteBuffer
  import java.text.SimpleDateFormat
  import java.util.{ Calendar, Date, TimeZone }
  import scala.concurrent.{ Await, Future, future }
  import scala.concurrent.duration._
  import scala.concurrent.ExecutionContext.Implicits._
  import scala.language.postfixOps
  import scala.io.Source
  import scala.util.{ Try, Success, Failure }
  import org.apache.zookeeper.{ Watcher, WatchedEvent }

  // state variables
  private var debugOn = false

  // session history
  private val history = new History(rt.maxHistory)
  history.load(rt.historyFile)

  // define a custom tabular instance
  private val tabular = new Tabular() with AvroTables

  // global date parser
  private val sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss")

  // redirect STDOUT and STDERR to my own buffers
  private val out = System.out
  private val err = System.err
  private val baos = new ByteArrayOutputStream(16384)
  System.setOut(new PrintStream(baos))

  // session variables
  private var zkcwd = "/"

  // create the ZooKeeper proxy
  out.println(s"Connecting to ZooKeeper at '${rt.zkEndPoint}'...")
  private val zk = ZKProxy(rt.zkEndPoint, new Watcher {
    override def process(event: WatchedEvent) = ()
  })

  // make sure we shutdown the ZooKeeper connection
  Runtime.getRuntime().addShutdownHook(new Thread {
    override def run() = zk.close
  })

  // get the list of brokers from zookeeper
  private val brokers: Seq[Broker] = KafkaSubscriber.getBrokerList(zk) map (b => Broker(b.host, b.port))

  /**
   * Interactive shell
   */
  def shell() {
    val userName = scala.util.Properties.userName

    do {
      // display the prompt, and get the next line of input
      out.print("%s@%s:%s> ".format(userName, remoteHost, zkcwd))
      val line = Console.readLine.trim()

      if (line.nonEmpty) {
        Try(interpret(line)) match {
          case Success(result) =>
            handleResult(result)
            history += line
          case Failure(e: IllegalArgumentException) =>
            if (debugOn) e.printStackTrace()
            err.println(s"Syntax error: ${e.getMessage}")
          case Failure(e) =>
            if (debugOn) e.printStackTrace()
            err.println(s"Runtime error: ${e.getMessage}")
        }
      }
    } while (rt.alive)
  }

  /**
   * Dumps the contents of the given file
   * Example: cat avro/schema1.avsc
   */
  def cat(args: String*): Seq[String] = {
    import scala.io.Source

    // get the file path
    val path = args(0)
    Source.fromFile(path).getLines().toSeq
  }

  def debug(args: String*): String = {
    if (args.isEmpty) debugOn = !debugOn else debugOn = args(0).toBoolean
    s"debugging is ${if (debugOn) "On" else "Off"}"
  }

  /**
   * "exit" command - Exits the shell
   */
  def exit(args: String*) = {
    rt.alive = false
    history.store(rt.historyFile)
  }

  /**
   * Inspects the classpath for the given resource by name
   * Example: resource org/apache/http/message/BasicLineFormatter.class
   */
  def findResource(args: String*): String = {
    // get the class name (with slashes)
    val path = args(0)
    val index = path.lastIndexOf('.')
    val resourceName = path.substring(0, index).replace('.', '/') + path.substring(index)
    logger.info(s"resource path is '$resourceName'")

    // determine the resource
    val classLoader = VerifyShell.getClass.getClassLoader()
    val resource = classLoader.getResource(resourceName)
    String.valueOf(resource)
  }

  /**
   * Inspects a class using reflection
   * Example: class org.apache.commons.io.IOUtils -m
   */
  def inspectClass(args: String*): Seq[String] = {
    val className = extract(args, 0).getOrElse(getClass.getName).replace('/', '.')
    val action = extract(args, 1) getOrElse ("-m")
    val beanClass = Class.forName(className)

    action match {
      case "-m" => beanClass.getDeclaredMethods() map (_.toString)
      case "-f" => beanClass.getDeclaredFields() map (_.toString)
      case _ => beanClass.getDeclaredMethods() map (_.toString)
    }
  }

  /**
   * "help" command - Provides the list of available commands
   */
  def help(args: String*): Seq[String] = {
    COMMANDS.toSeq filter {
      case (name, cmd) => args.isEmpty || name.startsWith(args(0))
    } sortBy (_._1) map {
      case (name, cmd) =>
        f"$name%-10s ${cmd.help}"
    }
  }

  def executeHistory(args: String*) {
    for {
      index <- args.headOption map (_.toInt)
      line <- history(index - 1)
    } {
      val result = interpret(line)
      handleResult(result)
    }
  }

  def listHistory(args: String*): Seq[String] = {
    val lines = history.getLines
    val data = ((1 to lines.size) zip lines) map {
      case (itemNo, command) => HistoryItem(itemNo, command)
    }
    tabular.transform(data)
  }

  /**
   * "hostname" command - Returns the name of the current host
   */
  def hostname(args: String*): String = {
    java.net.InetAddress.getLocalHost().getHostName()
  }

  /**
   * "ps" command - Display a list of "configured" running processes
   */
  def processList(args: String*): Seq[String] = {
    import scala.util.Properties

    // get the node
    val node = extract(args, 0) getOrElse "."
    val timeout = extract(args, 1) map (_.toInt) getOrElse 60
    out.println(s"Gathering process info from host: ${if (node == ".") "localhost" else node}")

    // parse the process and port mapping data
    val outcome = for {
    // retrieve the process and port map data
      (psdata, portmap) <- remoteData(node)

      // process the raw output
      lines = psdata map (seq => if (Properties.isMac) seq.tail else seq)

      // filter the data, and produce the results
      result = lines filter (s => s.contains("mysqld") || s.contains("java") || s.contains("python")) flatMap { s =>
        s match {
          case PID_MacOS_r(user, pid, _, _, time1, _, time2, cmd, args) => Some(parsePSData(pid, cmd, args, portmap.get(pid)))
          case PID_Linux_r(user, pid, _, _, time1, _, time2, cmd, args) => Some(parsePSData(pid, cmd, args, portmap.get(pid)))
          case _ => None
        }
      }
    } yield result

    // and let's wait for the result... 
    Await.result(outcome, timeout seconds)
  }

  /**
   * "pkill" command - Terminates specific running processes
   */
  def processKill(args: String*): String = {
    import scala.sys.process._

    // get the PIDs
    val pids = args

    // kill the processes
    s"kill ${pids mkString (" ")}".!!
  }

  /**
   * Parses process data produced by the UNIX "ps" command
   */
  private def parsePSData(pid: String, cmd: String, args: String, portCmd: Option[String]): String = {
    val command = cmd match {
      case s if s.contains("mysqld") => "MySQL Server"
      case s if s.endsWith("java") =>
        args match {
          case a if a.contains("cassandra") => "Cassandra"
          case a if a.contains("kafka") => "Kafka"
          case a if a.contains("mysqld") => "MySQLd"
          case a if a.contains("tesla-stream") => "Verify"
          case a if a.contains("storm nimbus") => "Storm Nimbus"
          case a if a.contains("storm supervisor") => "Storm Supervisor"
          case a if a.contains("storm ui") => "Storm UI"
          case a if a.contains("storm") => "Storm"
          case a if a.contains("/usr/local/java/zookeeper") => "Zookeeper"
          case _ => s"java [$args]"
        }
      case s =>
        args match {
          case a if a.contains("storm nimbus") => "Storm Nimbus"
          case a if a.contains("storm supervisor") => "Storm Supervisor"
          case a if a.contains("storm ui") => "Storm UI"
          case _ => s"$cmd [$args]"
        }
    }

    portCmd match {
      case Some(port) => f"$pid%6s $command <$port>"
      case _ => f"$pid%6s $command"
    }
  }

  /**
   * Retrieves "netstat -ptln" and "ps -ef" data from a remote node
   * @param node the given remote node (e.g. "Verify")
   * @return a future containing the data
   */
  private def remoteData(node: String): Future[(Seq[String], Map[String, String])] = {
    import scala.io.Source
    import scala.sys.process._

    // asynchronously get the raw output from 'ps -ef'
    val psdataF: Future[Seq[String]] = future {
      Source.fromString((node match {
        case "." => "ps -ef"
        case host => s"ssh -i /home/ubuntu/dev.pem ubuntu@$host ps -ef"
      }).!!).getLines.toSeq
    }

    // asynchronously get the port mapping
    val portmapF: Future[Map[String, String]] = future {
      // get the lines of data from 'netstat'
      val netstat = (Source.fromString((node match {
        case "." => "netstat -ptln"
        case host => s"ssh -i /home/ubuntu/dev.pem ubuntu@$host netstat -ptln"
      }).!!)).getLines.toSeq.tail

      // build the port mapping
      netstat flatMap {
        _ match {
          case NETSTAT_r(_, _, _, rawport, _, _, pidcmd, _*) =>
            if (pidcmd.contains("java")) {
              val port = rawport.substring(rawport.lastIndexOf(':') + 1)
              val Array(pid, cmd) = pidcmd.trim.split("[/]")
              Some((port, pid, cmd))
            } else None
          case _ => None
        }
      } map {
        case (port, pid, cmd) => (pid -> port)
      } groupBy (_._1) map {
        case (pid, seq) => (pid, seq.sortBy(_._2).reverse map (_._2) mkString (", "))
      }
    }

    // let's combine the futures
    for {
      pasdata <- psdataF
      portmap <- portmapF
    } yield (pasdata, portmap)
  }

  /**
   * Re-establishes the connection to Zookeeper
   */
  def reconnect(args: String*) = zk.reconnect()

  /**
   * "storm" command - Deploys a topology to the Storm server
   * Example: storm Verify-stream-server.jar org.Verify.stream.oooi.VerifyStreamFlights oooi-config.properties
   */
  def stormDeploy(args: String*): String = {
    import scala.sys.process._

    // deploy the topology
    s"storm jar ${args.mkString(" ")}".!!
  }

  /**
   * "systime" command - Returns the system time as an EPOC in milliseconds
   */
  def systime(args: String*) = System.currentTimeMillis.toString()

  /**
   * "time" command - Returns the time in the local time zone
   */
  def time(args: String*): Date = new Date()

  /**
   * "timeutc" command - Returns the time in the GMT time zone
   */
  def timeUTC(args: String*): String = {
    import java.text.SimpleDateFormat

    val fmt = new SimpleDateFormat("E MMM dd HH:mm:ss z yyyy");
    fmt.setTimeZone(TimeZone.getTimeZone("GMT"))
    fmt.format(new Date())
  }

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
    new KafkaSubscriber(Topic(name, partition.toInt), brokers) use (_.commitOffsets(groupId, offset.toLong, metadata))
  }

  /**
   * "kcount" - Returns the number of available messages for a given topic
   */
  def topicCount(args: String*): Option[Long] = {
    // get the arguments
    val Seq(name, partition, _*) = args

    // determine the difference between the first and last offsets
    for {
      first <- topicFirstOffset(args: _*)
      last <- topicLastOffset(args: _*)
    } yield last - first
  }

  /**
   * "kstats" - Returns the number of available messages for a given topic
   */
  def topicStats(args: String*): Seq[TopicOffsets] = {
    // get the arguments
    val Seq(name, beginPartition, endPartition, _*) = args

    // determine the difference between the first and last offsets
    for {
      partition <- (beginPartition.toInt to endPartition.toInt)
      first <- topicFirstOffset(name, partition.toString)
      last <- topicLastOffset(name, partition.toString)
    } yield TopicOffsets(name, partition, first, last, last - first)
  }

  /**
   * "kmk" - Creates a new topic
   */
  def topicCreate(args: String*) {
    import org.I0Itec.zkclient.ZkClient
    import _root_.kafka.admin.AdminUtils

    val Seq(topic, partitions, replicas, _*) = args
    val topicConfig = new java.util.Properties()

    new ZkClient(remoteHost) use (AdminUtils.createTopic(_, topic, partitions.toInt, replicas.toInt, topicConfig))
  }

  /**
   * "krm" - Deletes a new topic
   */
  def topicDelete(args: String*) {
    import org.I0Itec.zkclient.ZkClient
    import _root_.kafka.admin.AdminUtils

    val Seq(topic, _*) = args

    new ZkClient(remoteHost) use (AdminUtils.deleteTopic(_, topic))
  }

  /**
   * "kdump" - Dumps the contents of a specific topic to the console [as binary]
   * Example: kdump topics.ldaniels528.test1 0 58500700 58500724
   */
  def topicDumpBinary(args: String*): Long = {
    // get the arguments
    val Seq(name, partition, _*) = args
    val startOffset = extract(args, 2) map (_.toLong)
    val endOffset = extract(args, 3) map (_.toLong)
    val blockSize = extract(args, 4) map (_.toInt)

    // perform the action
    new KafkaSubscriber(Topic(name, partition.toInt), brokers) use {
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
   * "kavrofields" - Returns the fields of an Avro message from a Kafka topic
   * Example1: kavrofields avro/schema1.avsc topics.ldaniels528.test1 0 58500700
   * Example2: kavrofields avro/schema2.avsc topics.ldaniels528.test2 9 1799020
   */
  def topicAvroFields(args: String*): Seq[String] = {
    import java.io.File
    import scala.collection.mutable.Buffer
    import scala.collection.JavaConversions._
    import scala.io.Source
    import org.apache.avro.generic.GenericRecord

    // get the arguments
    val Seq(schemaPath, name, partition, _*) = args
    val offset = extract(args, 3) map (_.toLong)
    val blockSize = extract(args, 4) map (_.toInt)

    // mske sure the file exists
    val schemaFile = new File(schemaPath)
    if (!schemaFile.exists()) {
      throw new IllegalStateException(s"Schema file '${schemaFile.getAbsolutePath()}' not found")
    }

    // retrieve the schema as a string
    val schemaString = Source.fromFile(schemaFile).getLines() mkString ("\n")
    val decoder = new AvroDecoder(schemaString)
    var fields: Seq[String] = Seq.empty

    // perform the action
    new KafkaSubscriber(Topic(name, partition.toInt), brokers) use {
      _.consume(offset, offset map (_ + 1), blockSize, new MessageConsumer {
        override def consume(offset: Long, message: Array[Byte]) {
          decoder.decode(message) match {
            case Success(record) =>
              fields = record.getSchema().getFields().map(_.name.trim).toSeq
            case Failure(e) =>
              out.println("[%04d] %s".format(offset, e.getMessage()))
          }
        }
      })
    }

    fields
  }

  /**
   * "kdumpa" - Dumps the contents of a specific topic to the console [as AVRO]
   * Example1: kdumpa avro/schema1.avsc topics.ldaniels528.test1 0 58500700 58500724
   * Example2: kdumpa avro/schema2.avsc topics.ldaniels528.test2 9 1799020 1799029 1024 field1+field2+field3+field4
   */
  def topicDumpAvro(args: String*): Long = {
    import java.io.File
    import scala.collection.mutable.Buffer
    import scala.io.Source
    import org.apache.avro.generic.GenericRecord

    // get the arguments
    val Seq(schemaPath, name, partition, _*) = args
    val startOffset = extract(args, 3) map (_.toLong)
    val endOffset = extract(args, 4) map (_.toLong)
    val blockSize = extract(args, 5) map (_.toInt)
    val fields: Seq[String] = extract(args, 6) map (_.split("[+]")) map (_.toSeq) getOrElse Seq.empty

    // mske sure the file exists
    val schemaFile = new File(schemaPath)
    if (!schemaFile.exists()) {
      throw new IllegalStateException(s"Schema file '${schemaFile.getAbsolutePath()}' not found")
    }

    // retrieve the schema as a string
    val schemaString = Source.fromFile(schemaFile).getLines() mkString ("\n")
    val decoder = new AvroDecoder(schemaString)
    val records = Buffer[GenericRecord]()

    // perform the action
    val offset = new KafkaSubscriber(Topic(name, partition.toInt), brokers) use {
      _.consume(startOffset, endOffset, blockSize, new MessageConsumer {
        override def consume(offset: Long, message: Array[Byte]) {
          decoder.decode(message) match {
            case Success(record) =>
              records += record
              ()
            case Failure(e) =>
              out.println("[%04d] %s".format(offset, e.getMessage()))
          }
        }
      })
    }

    // transform the records into a table
    tabular.transformAvro(records, fields) foreach (out.println)

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
    new KafkaSubscriber(Topic(name, partition.toInt), brokers) use {
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
    val counts = extract(args, 5) map(_.toLowerCase) map(s => s == "-c") getOrElse false
    val blockSize = extract(args, 6) map (_.toInt)

    // output the output file
    var count = 0L
    new DataOutputStream( new FileOutputStream(file)) use { fos =>
      // perform the action
      new KafkaSubscriber(Topic(name, partition.toInt), brokers) use {
        _.consume(startOffset, endOffset, blockSize, new MessageConsumer {
          override def consume(offset: Long, message: Array[Byte]) {
            if(counts) fos.writeInt(message.length)
            fos.write(message)
            count += 1
          }
        })
      }
    }
    count
  }

  def topicImport(args: String*) = {
    // get the arguments
    val Seq(topic, fileType, filePath, _*) = args

    new KafkaPublisher() use { publisher =>
      publisher.open(brokers)

      // process based on file type
      fileType.toLowerCase() match {
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
      publisher.publish(topic, System.currentTimeMillis().toString, message.getBytes("UTF-8"))
    }
  }

  private def topicImportAvroFile(publisher: KafkaPublisher, filePath: String) {
    import java.io.File
    import org.apache.avro.Schema
    import org.apache.avro.file.DataFileReader
    import org.apache.avro.generic.{ GenericDatumReader, GenericDatumWriter, GenericRecord }
    import org.apache.avro.io.{ BinaryDecoder, DecoderFactory, EncoderFactory }

    val reader = new DataFileReader[GenericRecord](new File(filePath), new GenericDatumReader[GenericRecord]())
    while (reader.hasNext()) {
      val record = reader.next()
      for {
        topic <- Option(record.get("topic")) map (_.toString)
        partition <- Option(record.get("partition"))
        offset <- Option(record.get("offset")) map (_.toString)
        buf <- Option(record.get("message")) map (_.asInstanceOf[java.nio.Buffer])
        message = buf.array().asInstanceOf[Array[Byte]]
      } {
        publisher.publish(topic, offset, message)
      }
    }
  }

  /**
   * "kls" - Lists all existing topicList
   */
  def topicList(args: String*): Seq[TopicDetail] = {
    val prefix = args.headOption

    KafkaSubscriber.listTopics(zk, brokers) flatMap { t =>
      val detail = TopicDetail(t.topic, t.partitionId, t.leader map (_.toString) getOrElse ("N/A"), t.replicas.size)
      if (prefix.isEmpty || prefix.map(t.topic.startsWith(_)).getOrElse(false)) Some(detail) else None
    }
  }

  /**
   * "kfetch" - Returns the offsets for a given topic and group ID
   */
  def topicFetchOffsets(args: String*): Option[Long] = {
    // get the arguments
    val Seq(name, partition, groupId, _*) = args

    // perform the action
    new KafkaSubscriber(Topic(name, partition.toInt), brokers) use (_.fetchOffsets(groupId))
  }

  /**
   * "kget" - Returns the offsets for a given topic and group ID
   */
  def topicGetMessage(args: String*) {
    // get the arguments
    val Seq(name, partition, offset, _*) = args
    val fetchSize = extract(args, 3) map (_.toInt) getOrElse 1024

    // perform the action
    new KafkaSubscriber(Topic(name, partition.toInt), brokers) use {
      _.fetch(offset.toLong, fetchSize).headOption map { m =>
        m.message.sliding(40, 40) foreach { bytes =>
          out.println("[%04d] %-80s %-40s".format(m.offset, asHexString(bytes), asChars(bytes)))
        }
      }
    }
  }

  /**
   * "kfirst" - Returns the first offset for a given topic
   */
  def topicFirstOffset(args: String*): Option[Long] = {
    // get the arguments
    val Seq(name, partition, _*) = args

    // perform the action
    new KafkaSubscriber(Topic(name, partition.toInt), brokers) use (_.getFirstOffset)
  }

  /**
   * "klast" - Returns the last offset for a given topic
   */
  def topicLastOffset(args: String*): Option[Long] = {
    // get the arguments
    val Seq(name, partition, _*) = args

    // perform the action
    new KafkaSubscriber(Topic(name, partition.toInt), brokers) use (_.getLastOffset)
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
    new KafkaSubscriber(Topic(name, partition.toInt), brokers) use (_.getOffsetsBefore(sysTimeMillis))
  }

  /**
   * "kpush" - Returns the EOF offset for a given topic
   */
  def topicPublish(args: String*): Unit = {
    // get the arguments
    val Seq(name, key, _*) = args

    out.println("Type the message and press ENTER:")
    out.print(">> ")
    val message = Console.readLine.trim()

    // publish the message
    new KafkaPublisher() use { publisher =>
      publisher.open(brokers)
      publisher.publish(name, key, message.getBytes())
    }
  }

  /**
   * "kwatch" - Subscribes to a specific topic
   */
  def topicWatch(args: String*): Long = {
    import scala.concurrent.duration.FiniteDuration._

    // get the arguments
    val Seq(name, partition, _*) = args
    val duration = (extract(args, 2) map (_.toInt) getOrElse 60).seconds

    // perform the action
    var count = 0L
    KafkaSubscriber.watch(Topic(name, partition.toInt), brokers, None, duration,
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
    import scala.concurrent.duration.FiniteDuration._

    // get the arguments
    val Seq(name, partition, groupId, _*) = args
    val duration = (extract(args, 3) map (_.toInt) getOrElse 60).seconds

    // perform the action
    var count = 0L
    KafkaSubscriber.watchGroup(Topic(name, partition.toInt), brokers, groupId, duration,
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
   * "version" - Returns the application version
   * @return the application version
   */
  def version(args: String*) = VERSION

  /**
   * "wpost" - Transfers the text data (via HTTP POST) from a file to a web-service end-point
   * (e.g. 'wpost flights.txt http://localhost:9000/stream/api/flights/publish')
   * (e.g. 'wpost events.txt http://localhost:9000/stream/api/events/publish')
   */
  def wPost(args: String*): Long = {
    val Seq(file, url, _*) = args
    val verbose = (args.length > 2)
    val throttle = extract(args, 3) map (_.toLong)

    // transfer the file's contents
    var count = 0L
    Source.fromFile(file).getLines foreach { line =>
      if (line.trim.length > 0) {
        count += 1
        if (verbose) {
          out.println("[%04d] %s".format(count, line))
        }
        httpPost(url, line)
        throttle foreach (Thread.sleep)
      }
    }
    count
  }

  /**
   * "zruok" - Checks the status of a Zookeeper instance
   * (e.g. echo ruok | nc zookeeper 2181)
   */
  def zkRuok(args: String*): String = {
    import scala.sys.process._

    // echo ruok | nc zookeeper 2181
    "echo ruok" #> s"nc ${rt.zkEndPoint.host} ${rt.zkEndPoint.port}" !!
  }

  /**
   * "zstat" - Returns the statistics of a Zookeeper instance
   * echo stat | nc zookeeper 2181
   */
  def zkStat(args: String*): String = {
    import scala.sys.process._

    // echo stat | nc zookeeper 2181
    "echo stat" #> s"nc ${rt.zkEndPoint.host} ${rt.zkEndPoint.port}" !!
  }

  /**
   * "zcd" - Changes the current path/directory in ZooKeeper
   */
  def zkChangeDir(args: String*): String = {
    // get the argument
    val key = args(0)

    // perform the action
    zkcwd = key match {
      case s if s == ".." =>
        zkcwd.split("[/]") match {
          case a if a.length <= 1 => "/"
          case a =>
            val newpath = a.init.mkString("/")
            if (newpath.trim.length == 0) "/" else newpath
        }
      case s => zkKeyToPath(s)
    }
    zkcwd
  }

  /**
   * "zls" - Retrieves the child nodes for a key from ZooKeeper
   */
  def zkChildren(args: String*): Seq[String] = {
    // get the argument
    val path = if (args.nonEmpty) zkKeyToPath(args(0)) else zkcwd

    // perform the action
    zk.getChildren(path, false)
  }

  /**
   * "zrm" - Removes a key-value from ZooKeeper
   */
  def zkDelete(args: String*) {
    // get the argument
    val key = args(0)

    // convert the key to a fully-qualified path
    val path = zkKeyToPath(key)

    // perform the action
    zk.exists(path) foreach (zk.delete(path, _))
  }

  /**
   * "zexists" - Removes a key-value from ZooKeeper
   */
  def zkExists(args: String*): Seq[String] = {
    // get the argument
    val key = args(0)

    // convert the key to a fully-qualified path
    val path = zkKeyToPath(key)

    // perform the action
    zk.exists(path) match {
      case Some(stat) =>
        Seq(
          s"Aversion: ${stat.getAversion()}",
          s"version: ${stat.getVersion()}",
          s"data length: ${stat.getDataLength()}",
          s"children: ${stat.getNumChildren()}",
          s"change time: ${new Date(stat.getCtime())}")
      case None =>
        Seq.empty
    }
  }

  /**
   * "zget" - Sets a key-value in ZooKeeper
   */
  def zkGet(args: String*): Option[String] = {
    // get the arguments
    val Seq(key, typeName, _*) = args

    // convert the key to a fully-qualified path
    val path = zkKeyToPath(key)

    // perform the action
    zk.exists(path) map (zk.read(path, _)) match {
      case Some(bytes) => Some(fromBytes(bytes, typeName))
      case None => None
    }
  }

  private def zkKeyToPath(key: String): String = {
    key match {
      case s if s.startsWith("/") => key
      case s => (if (zkcwd.endsWith("/")) zkcwd else zkcwd + "/") + s
    }
  }

  /**
   * "zmk" - Creates a new ZooKeeper sub-directory (key)
   */
  def zkMkdir(args: String*) = {
    // get the arguments
    val Seq(key, _*) = args

    // perform the action
    zk.ensurePath(zkKeyToPath(key))
  }

  /**
   * "zput" - Retrieves a value from ZooKeeper
   */
  def zkPut(args: String*) = {
    // get the arguments
    val Seq(key, value, typeName, _*) = args

    // convert the key to a fully-qualified path
    val path = zkKeyToPath(key)

    // perform the action
    zk.exists(path) foreach (zk.delete(path, _))
    zk.ensureParents(path).create(path -> toBytes(value, typeName))
  }

  /**
   * "zsession" - Retrieves the Session ID from ZooKeeper
   */
  def zkSession(args: String*) = zk.getSessionId().toString

  private def checkArgs(command: Command, args: Seq[String]): Seq[String] = {
    // determine the minimum and maximum number of parameters
    val minimum = command.params._1.size
    val maximum = minimum + command.params._2.size

    // make sure the arguments are within bounds
    if (args.length < minimum || args.length > maximum) {
      throw new IllegalArgumentException(s"Usage: ${command.prototype}")
    }

    args
  }

  private def handleResult(result: Any) {
    result match {
      case s: Seq[_] if !Tabular.isPrimitives(s) => tabular.transform(s) foreach (out.println)
      case t: scala.collection.GenTraversableOnce[_] => t foreach (out.println)
      case o: Option[_] => o foreach (out.println)
      case x => if (x != null && !x.isInstanceOf[Unit]) {
        out.println(x)
      }
    }
  }

  private def interpret(input: String): Any = {
    // parse & evaluate the user input
    parseInput(input) match {
      case Some((cmd, args)) =>
        // match the command
        COMMANDS.get(cmd) match {
          case Some(command) =>
            checkArgs(command, args)
            command.fx(args)
          case _ =>
            throw new IllegalArgumentException(s"'$input' not recognized")
        }
      case _ =>
    }
  }

  private def parseInput(input: String): Option[(String, Seq[String])] = {
    // parse the user input
    val pcs = CommandParser.parse(input)

    // return the command and arguments
    for {
      cmd <- pcs.headOption map (_.toLowerCase)
      args = pcs.tail
    } yield (cmd, args)
  }

  /**
   * Executes a Java application via its "main" method
   * @param className the name of the class to invoke
   * @param args the arguments to pass to the application
   */
  private def runJava(className: String, args: String*): Iterator[String] = {
    // reset the buffer
    baos.reset()

    // execute the command
    val commandClass = Class.forName(className)
    val mainMethod = commandClass.getMethod("main", classOf[Array[String]])
    mainMethod.invoke(null, args.toArray)

    // return the iteration of lines
    Source.fromBytes(baos.toByteArray()).getLines
  }

  private def asChars(bytes: Array[Byte]): String = {
    String.valueOf(bytes map (b => if (b >= 32 && b <= 126) b.toChar else '.'))
  }

  private def fromBytes(bytes: Array[Byte], typeName: String): String = {
    typeName match {
      case "hex" => bytes map (b => "%02x".format(b)) mkString (".")
      case "int" => ByteBuffer.wrap(bytes).getInt().toString
      case "long" => ByteBuffer.wrap(bytes).getLong().toString
      case "dec" | "double" => ByteBuffer.wrap(bytes).getDouble().toString
      case "string" | "text" => new String(bytes)
      case _ => throw new IllegalArgumentException(s"Invalid type '$typeName'")
    }
  }

  private def toBytes(value: String, typeName: String): Array[Byte] = {
    import ZKProxy.Implicits._
    import ByteBuffer.allocate
    typeName match {
      case "hex" => value.getBytes()
      case "int" => allocate(4).putInt(value.toInt)
      case "long" => allocate(8).putLong(value.toLong)
      case "dec" | "double" => allocate(8).putDouble(value.toDouble)
      case "string" | "text" => value.getBytes()
      case _ => throw new IllegalArgumentException(s"Invalid type '$typeName'")
    }
  }

  /**
   * Pre-define all commands
   */
  private val COMMANDS = Map[String, Command](Seq(
    Command("exit", exit, help = "Exits the shell"),
    Command("!", executeHistory, (Seq("index"), Seq.empty), help = "Executes a previously issued command"),
    Command("?", help, (Seq.empty, Seq("search-term")), help = "Provides the list of available commands"),
    Command("cat", cat, (Seq("file"), Seq.empty), help = "Dumps the contents of the given file"),
    Command("class", inspectClass, (Seq.empty, Seq("action")), help = "Inspects a class using reflection"),
    Command("debug", debug, (Seq.empty, Seq("state")), help = "Switches debugging on/off"),
    Command("help", help, help = "Provides the list of available commands"),
    Command("history", listHistory, help = "Returns a list of previously issued commands"),
    Command("hostname", hostname, help = "Returns the name of the current host"),
    Command("kavrofields", topicAvroFields, (Seq("schemaPath", "topic", "partition"), Seq("offset", "blockSize")), help = "Returns the fields of an Avro message from a Kafka topic"),
    Command("kbrokers", topicBrokers, (Seq.empty, Seq.empty), help = "Returns a list of the registered brokers from ZooKeeper"),
    Command("kcommit", topicCommit, (Seq("topic", "partition", "groupId", "offset"), Seq("metadata")), "Commits the offset for a given topic and group"),
    Command("kcount", topicCount, (Seq("topic", "partition"), Seq.empty), help = "Returns the number of messages available for a given topic"),
    Command("kdump", topicDumpBinary, (Seq("topic", "partition"), Seq("startOffset", "endOffset", "blockSize")), "Dumps the contents of a specific topic [as binary] to the console"),
    Command("kdumpa", topicDumpAvro, (Seq("schemaPath", "topic", "partition"), Seq("startOffset", "endOffset", "blockSize", "fields")), "Dumps the contents of a specific topic [as Avro] to the console"),
    Command("kdumpr", topicDumpRaw, (Seq("topic", "partition"), Seq("startOffset", "endOffset", "blockSize")), "Dumps the contents of a specific topic [as raw ASCII] to the console"),
    Command("kdumpf", topicDumpToFile, (Seq("file", "topic", "partition"), Seq("startOffset", "endOffset", "flags", "blockSize")), "Dumps the contents of a specific topic to a file"),
    Command("kfetch", topicFetchOffsets, (Seq("topic", "partition", "groupId"), Seq.empty), "Retrieves the offset for a given topic and group"),
    Command("kfirst", topicFirstOffset, (Seq("topic", "partition"), Seq.empty), help = "Returns the first offset for a given topic"),
    Command("kget", topicGetMessage, (Seq("topic", "partition", "offset"), Seq("fetchSize")), help = "Retrieves a single record at the specified offset for a given topic"),
    Command("kimport", topicImport, (Seq("topic", "fileType", "filePath"), Seq.empty), "Imports data into a new/existing topic"),
    Command("klast", topicLastOffset, (Seq("topic", "partition"), Seq.empty), help = "Returns the last offset for a given topic"),
    Command("kls", topicList, (Seq.empty, Seq("prefix")), help = "Lists all existing topics"),
    Command("kmk", topicCreate, (Seq("topic", "partitions", "replicas"), Seq.empty), "Returns the system time as an EPOC in milliseconds"),
    Command("koffset", topicOffset, (Seq("topic", "partition"), Seq("time=YYYY-MM-DDTHH:MM:SS")), "Returns the offset at a specific instant-in-time for a given topic"),
    Command("kpush", topicPublish, (Seq("topic", "key"), Seq.empty), "Publishes a message to a topic"),
    Command("krm", topicDelete, (Seq("topic"), Seq.empty), "Deletes a topic"),
    Command("kstats", topicStats, (Seq("topic", "beginPartition", "endPartition"), Seq.empty), help = "Returns the parition details for a given topic"),
    Command("kwatch", topicWatch, (Seq("topic", "partition"), Seq("timeout-in-seconds")), "Subscribes to a specific topic"),
    Command("pkill", processKill, (Seq("pid0"), Seq("pid1", "pid2", "pid3", "pid4", "pid5", "pid6")), help = "Terminates specific running processes"),
    Command("ps", processList, (Seq.empty, Seq("node", "timeout")), help = "Display a list of \"configured\" running processes"),
    Command("reconnect", reconnect, (Seq.empty, Seq.empty), help = "Re-establishes the connection to Zookeeper"),
    Command("resource", findResource, (Seq("resourcename"), Seq.empty), help = "Inspects the classpath for the given resource"),
    Command("storm", stormDeploy, (Seq("jarfile", "topology"), Seq("arguments")), help = "Deploys a topology to the Storm server"),
    Command("systime", systime, help = "Returns the system time as an EPOC in milliseconds"),
    Command("time", time, help = "Returns the system time"),
    Command("timeutc", timeUTC, help = "Returns the system time in UTC"),
    Command("version", version, help = "Returns the Verify application version"),
    Command("wpost", wPost, (Seq("file", "url"), Seq("verbose", "throttle")), "Transfers the text data (via HTTP POST) from a file to a web-service end-point"),
    Command("zcd", zkChangeDir, (Seq("key"), Seq.empty), help = "Changes the current path/directory in ZooKeeper"),
    Command("zexists", zkExists, (Seq("key"), Seq.empty), "Removes a key-value from ZooKeeper"),
    Command("zget", zkGet, (Seq("key", "type"), Seq.empty), "Sets a key-value in ZooKeeper"),
    Command("zls", zkChildren, (Seq.empty, Seq("path")), help = "Retrieves the child nodes for a key from ZooKeeper"),
    Command("zmk", zkMkdir, (Seq("key"), Seq.empty), "Creates a new ZooKeeper sub-directory (key)"),
    Command("zput", zkPut, (Seq("key", "value", "type"), Seq.empty), "Retrieves a value from ZooKeeper"),
    Command("zrm", zkDelete, (Seq("key"), Seq.empty), "Removes a key-value from ZooKeeper"),
    Command("zruok", zkRuok, help = "Checks the status of a Zookeeper instance"),
    Command("zsess", zkSession, help = "Retrieves the Session ID from ZooKeeper"),
    Command("zstat", zkStat, help = "Returns the statistics of a Zookeeper instance")) map (c => (c.name, c)): _*)

  case class HistoryItem(itemNo: Int, command: String)

  case class MessageData(offset: Long, hex: String, chars: String)

  case class TopicDetail(name: String, partition: Int, leader: String, version: Int)

  case class TopicOffsets(name: String, partition: Int, startOffset: Long, endOffset: Long, messagesAvailable: Long)

}

/**
 * Verify Console Shell Singleton
 * @author lawrence.daniels@gmail.com
 */
object VerifyShell {
  val VERSION = "1.0.4"

  private val logger = org.slf4j.LoggerFactory.getLogger(classOf[KafkaSubscriber])

  // define the process parsing regular expression
  val PID_MacOS_r = "^\\s*(\\d+)\\s*(\\d+)\\s*(\\d+)\\s*(\\S+)\\s*(\\S+)\\s*(\\S+)\\s*(\\S+)\\s*(.*)".r
  val PID_Linux_r = "^\\s*(\\S+)\\s*(\\d+)\\s*(\\d+)\\s*(\\d+)\\s*(\\S+)\\s*(\\S+)\\s*(\\S+)\\s*(\\S+)\\s*(.*)".r
  val NETSTAT_r = "^\\s*(\\S+)\\s*(\\S+)\\s*(\\S+)\\s*(\\S+)\\s*(\\S+)\\s*(\\S+)\\s*(.*)".r

  /**
   * Application entry point
   * @param args the given command line arguments
   */
  def main(args: Array[String]) {
    System.out.println(s"Verify Shell v$VERSION")

    // was a host argument passed?
    val host = extract(args, 0) getOrElse ("localhost")

    // create the runtime context
    val rt = VerifyShellRuntime.getInstance(host)

    // start the shell
    val console = new VerifyShell(host, rt)
    console.shell()
  }

  private def extract(args: Seq[String], index: Int): Option[String] = {
    if (args.length > index) Some(args(index)) else None
  }

  /**
   * Represents an Verify Shell command
   * @author lawrence.daniels@gmail.com
   */
  case class Command(name: String, fx: Seq[String] => Any, params: (Seq[String], Seq[String]) = (Seq.empty, Seq.empty), help: String = "") {

    def prototype = {
      val required = (params._1 map (s => s"<$s>")).mkString(" ")
      val optional = (params._2 map (s => s"<$s>")).mkString(" ")
      s"$name $required ${if (optional.nonEmpty) s"[$optional]" else ""}"
    }
  }

}