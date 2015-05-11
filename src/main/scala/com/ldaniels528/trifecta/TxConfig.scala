package com.ldaniels528.trifecta

import java.io.{File, FileInputStream, FileOutputStream}
import java.util.Properties

import com.ldaniels528.trifecta.TxConfig._
import com.ldaniels528.trifecta.io.avro.AvroDecoder
import com.ldaniels528.commons.helpers.OptionHelper._
import com.ldaniels528.commons.helpers.PropertiesHelper._
import com.ldaniels528.commons.helpers.ResourceHelper._
import com.ldaniels528.commons.helpers.StringHelper._
import org.slf4j.LoggerFactory

import scala.collection.concurrent.TrieMap
import scala.io.Source
import scala.util.Properties._
import scala.util.{Failure, Success, Try}

/**
 * Trifecta Configuration
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class TxConfig(val configProps: Properties) {
  private lazy val logger = LoggerFactory.getLogger(getClass)
  private val cachedDecoders = TrieMap[File, TxDecoder]()

  // Initializes the configuration
  Try {
    // make sure the decoders and queries directories exist
    Seq(decoderDirectory, queriesDirectory) foreach { directory =>
      if (!directory.exists()) directory.mkdirs()
    }
  }

  // set the current working directory
  configProps.setProperty("trifecta.core.cwd", new File(".").getCanonicalPath)

  // the default state of the application is "alive"
  var alive = true

  // capture standard output
  val out = System.out
  val err = System.err

  // define the job manager
  val jobManager = new JobManager()

  // various shared state variables
  def autoSwitching: Boolean = configProps.getOrElse("trifecta.common.autoSwitching", "true").toBoolean

  def autoSwitching_=(enabled: Boolean): Unit = {
    configProps.setProperty("trifecta.common.autoSwitching", enabled.toString)
    ()
  }

  // the number of columns to display when displaying bytes
  def columns: Int = configProps.getOrElse("trifecta.common.columns", "25").toInt

  def columns_=(width: Int): Unit = {
    configProps.setProperty("trifecta.common.columns", width.toString)
    ()
  }

  def cwd: String = configProps.getProperty("trifecta.core.cwd")

  def cwd_=(path: String): Unit = {
    configProps.setProperty("trifecta.core.cwd", path)
    ()
  }

  def debugOn: Boolean = configProps.getOrElse("trifecta.common.debugOn", "false").toBoolean

  def debugOn_=(enabled: Boolean): Unit = {
    configProps.setProperty("trifecta.common.debugOn", enabled.toString)
    ()
  }

  def consumersPartitionManager: Boolean = configProps.getOrElse("trifecta.storm.kafka.consumers.partitionManager", "false").toBoolean

  def consumersPartitionManager_=(enabled: Boolean): Unit = {
    configProps.setProperty("trifecta.storm.kafka.consumers.partitionManager", enabled.toString)
    ()
  }

  def encoding: String = configProps.getOrElse("trifecta.common.encoding", "UTF-8")

  def encoding_=(charSet: String): Unit = {
    configProps.setProperty("trifecta.common.encoding", charSet)
    ()
  }

  // Zookeeper connection string
  def zooKeeperConnect: String = configProps.getOrElse("trifecta.zookeeper.host", "localhost:2181")

  def zooKeeperConnect_=(connectionString: String): Unit = {
    configProps.setProperty("trifecta.zookeeper.host", connectionString)
    ()
  }

  def kafkaRootPath: String = configProps.getOrElse("trifecta.zookeeper.kafka.root.path", "")

  def kafkaRootPath_=(path: String): Unit = {
    configProps.setProperty("trifecta.zookeeper.kafka.root.path", path)
    ()
  }

  /**
   * Attempts to retrieve decoders by topic
   * @return the collection of [[TxDecoder]] instances
   */
  def getDecodersByTopic(topic: String): Seq[TxDecoder] = {
    Option(new File(decoderDirectory, topic).listFiles)
      .map(_.toSeq map (getDecoderFromFile(topic, _)))
      .getOrElse(Nil)
      .sortBy(-_.lastModified)
  }

  /**
   * Returns all available decoders
   * @return the collection of [[TxDecoder]]s
   */
  def getDecoders: Seq[TxDecoder] = {
    Option(decoderDirectory.listFiles) map { topicDirectories =>
      (topicDirectories.toSeq flatMap { topicDirectory =>
        Option(topicDirectory.listFiles) map { decoderFiles =>
          decoderFiles map { decoderFile =>
            val topic = topicDirectory.getName
            getDecoderFromFile(topic, decoderFile)
          }
        }
      }).flatten
    } getOrElse Nil
  }

  private def getDecoderFromFile(topic: String, decoderFile: File): TxDecoder = {
    cachedDecoders.getOrElseUpdate(decoderFile, {
      val schema = Source.fromFile(decoderFile).getLines().mkString
      Try {
        TxDecoder(topic, decoderFile.getName, decoderFile.lastModified, Left(AvroDecoder(decoderFile.getName, schema)))
      } match {
        case Success(decoder) => decoder
        case Failure(e) => TxDecoder(topic, decoderFile.getName, decoderFile.lastModified, Right(TxFailedSchema(schema, e)))
      }
    })
  }

  def getQueriesByTopic(topic: String): Option[Seq[TxQuery]] = {
    Option(new File(queriesDirectory, topic).listFiles) map { queriesFiles =>
      queriesFiles map (getQueryFromFile(topic, _))
    }
  }

  private def getQueryFromFile(topic: String, file: File) = {
    val name = getQueryNameWithoutExtension(file.getName)
    TxQuery(name, topic, Source.fromFile(file).getLines().mkString("\n"), file.exists(), file.lastModified())
  }

  private def getQueryNameWithoutExtension(name: String) = {
    name.lastIndexOptionOf(".bdql") ?? name.lastIndexOptionOf(".kql") match {
      case Some(index) => name.substring(0, index)
      case None => name
    }
  }

  /**
   * Returns an option of the value corresponding to the given key
   * @param key the given key
   * @return an option of the value
   */
  def get(key: String): Option[String] = Option(configProps.getProperty(key))

  /**
   * Retrieves the value of the key or the default value
   * @param key the given key
   * @param default the given value
   * @return the value of the key or the default value
   */
  def getOrElse(key: String, default: => String): String = configProps.getOrElse(key, default)

  /**
   * Retrieves either the value of the key or the default value
   * @param key the given key
   * @param default the given value
   * @return either the value of the key or the default value
   */
  def getEither[T](key: String, default: => T): Either[String, T] = {
    Option(configProps.getProperty(key)) match {
      case Some(value) => Left(value)
      case None => Right(default)
    }
  }

  /**
   * Sets the value for the given key
   * @param key the given key
   * @param value the given value
   * @return an option of the previous value for the key
   */
  def set(key: String, value: String): Option[AnyRef] = Option(configProps.setProperty(key, value))

  /**
   * Saves the current configuration to disk
   * @param configFile the configuration file
   */
  def save(configFile: File): Unit = {
    Try {
      // if the parent directory doesn't exist, create it
      val parentDirectory = configFile.getParentFile
      if (!parentDirectory.exists()) {
        logger.info(s"Creating directory '${parentDirectory.getAbsolutePath}'...")
        parentDirectory.mkdirs()
      }

      // save the configuration file
      new FileOutputStream(configFile) use { fos =>
        configProps.store(fos, "Trifecta configuration properties")
      }
    }
    ()
  }

}

/**
 * Trifecta Configuration Singleton
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
object TxConfig {

  /**
   * Defines the directory for all Trifecta preferences
   */
  var trifectaPrefs: File = new File(new File(userHome), ".trifecta")

  /**
   * Returns the location of the history properties
   * @return the [[File]] representing the location of history properties
   */
  def historyFile: File = new File(trifectaPrefs, "history.txt")

  /**
   * Returns the location of the configuration properties
   * @return the [[File]] representing the location of configuration properties
   */
  def configFile: File = new File(trifectaPrefs, "config.properties")

  /**
   * Returns the location of the decoders directory
   * @return the [[File]] representing the location of the decoders directory
   */
  def decoderDirectory: File = new File(trifectaPrefs, "decoders")

  /**
   * Returns the location of the queries directory
   * @return the [[File]] representing the location of the queries directory
   */
  def queriesDirectory: File = new File(trifectaPrefs, "queries")

  /**
   * Returns the default configuration
   * @return the default configuration
   */
  def defaultConfig: TxConfig = new TxConfig(getDefaultProperties)

  /**
   * Loads the configuration file
   */
  def load(configFile: File): TxConfig = {
    val p = getDefaultProperties
    if (configFile.exists()) new FileInputStream(configFile) use (in => Try(p.load(in)))
    else new FileOutputStream(configFile) use (out => Try(p.store(out, "Trifecta configuration file")))
    new TxConfig(p)
  }

  private def getDefaultProperties: java.util.Properties = {
    Map(
      "trifecta.zookeeper.host" -> "localhost:2181",
      "trifecta.elasticsearch.hosts" -> "localhost",
      "trifecta.cassandra.hosts" -> "localhost ",
      "trifecta.storm.hosts" -> "localhost",
      "trifecta.common.autoSwitching" -> "true",
      "trifecta.common.columns" -> "25",
      "trifecta.common.debugOn" -> "false",
      "trifecta.common.encoding" -> "UTF-8").toProps
  }

  case class TxDecoder(topic: String, name: String, lastModified: Long, decoder: Either[AvroDecoder, TxFailedSchema])

  case class TxFailedSchema(schemaString: String, error: Throwable)

  case class TxQuery(name: String, topic: String, queryString: String, exists: Boolean, lastModified: Long)

}

