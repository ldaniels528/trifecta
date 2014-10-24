package com.ldaniels528.trifecta

import java.io.File._
import java.io.{File, FileInputStream, FileOutputStream}
import java.util.Properties

import com.ldaniels528.trifecta.util.TxUtils._
import com.ldaniels528.trifecta.vscript.RootScope

import scala.util.Properties._
import scala.util.Try

/**
 * Trifecta Configuration
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class TxConfig(val configProps: Properties) {
  // the default state of the application is "alive"
  var alive = true

  // capture standard output
  val out = System.out
  val err = System.err

  // create the root-level scope
  implicit val scope = RootScope()

  // define the job manager
  val jobManager = new JobManager()

  // Zookeeper connection string
  def zooKeeperConnect = configProps.getOrElse("trifecta.zookeeper.host", "127.0.0.1:2181")

  def kafkaZkConnect = configProps.getOrElse("trifecta.kafka.zookeeper.host", zooKeeperConnect)

  // various shared state variables
  def autoSwitching: Boolean = scope.getValue[Boolean]("autoSwitching") getOrElse false

  def autoSwitching_=(enabled: Boolean) = scope.setValue("autoSwitching", Option(enabled))

  // the number of columns to display when displaying bytes
  def columns: Int = getOrElse("columns", 25)

  def columns_=(width: Int): Unit = set("columns", width)

  def debugOn: Boolean = getOrElse("debugOn", false)

  def debugOn_=(enabled: Boolean): Unit = set("debugOn", enabled)

  def encoding: String = getOrElse("encoding", "UTF-8")

  def encoding_=(charSet: String): Unit = set("encoding", charSet)

  def get[T](name: String): Option[T] = scope.getValue[T](name)

  def getOrElse[T](name: String, default: T): T = scope.getValue[T](name) getOrElse default

  def set[T](name: String, value: T): Unit = scope.setValue[T](name, Option(value))

  /**
   * Saves the current configuration to disk
   * @param configFile the configuration file
   */
  def save(configFile: File): Unit = {
    new FileOutputStream(configFile) use { fos =>
      configProps.store(fos, "Trifecta configuration properties")
    }
  }

}

/**
 * Trifecta Configuration Singleton
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
object TxConfig {

  // define the history properties
  var historyFile = new File(s"$userHome$separator.trifecta${separator}history.txt")

  // define the configuration file & properties
  var configFile = new File(s"$userHome$separator.trifecta${separator}config.properties")

  /**
   * Returns the default configuration
   * @return the default configuration
   */
  def defaultConfig: TxConfig = new TxConfig(getDefaultProperties)

  /**
   * Loads the configuration file
   */
  def load(): TxConfig = {
    val p = getDefaultProperties
    if (configFile.exists()) {
      Try(p.load(new FileInputStream(configFile)))
    }
    new TxConfig(p)
  }

  private def getDefaultProperties: java.util.Properties = {
    Map(
      "trifecta.zookeeper.host" -> "localhost:2181",
      "trifecta.kafka.zookeeper.host" -> "localhost:2181",
      "trifecta.elasticsearch.hosts" -> "localhost",
      "trifecta.cassandra.hosts" -> "localhost ",
      "trifecta.storm.hosts" -> "localhost",
      "trifecta.autoSwitching" -> "true",
      "trifecta.columns" -> "25",
      "trifecta.debugOn" -> "true",
      "trifecta.encoding" -> "UTF-8").toProps
  }

}

