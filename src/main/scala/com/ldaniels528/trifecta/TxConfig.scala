package com.ldaniels528.trifecta

import java.io.File._
import java.io.{File, FileInputStream}
import java.util.concurrent.atomic.AtomicInteger
import java.util.{Properties, Random}

import com.ldaniels528.trifecta.TxConfig.JobItem
import com.ldaniels528.trifecta.vscript.RootScope

import scala.collection.mutable
import scala.concurrent.Future
import scala.util.Properties._
import scala.util.Try

/**
 * Trifecta Configuration
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class TxConfig() {
  // the default state of the application is "alive"
  var alive = true

  // capture standard output
  val out = System.out
  val err = System.err

  // create the root-level scope
  implicit val scope = RootScope()

  // define the job stack
  val jobs = mutable.Map[Int, JobItem]()

  // define the history properties
  var historyFile = new File(s"$userHome$separator.trifecta${separator}history.txt")

  // define the configuration file & properties
  val configFile = new File(s"$userHome$separator.trifecta${separator}config.properties")
  val configProps = loadConfiguration(configFile)

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
   * Loads the configuration file
   * @param configFile the configuration file
   */
  def loadConfiguration(configFile: File): Properties = {
    val p = new Properties()
    if (configFile.exists()) {
      Try(p.load(new FileInputStream(configFile)))
    }
    p
  }

}

/**
 * Trifecta Configuration Companion Object
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
object TxConfig {

  private val jobIdGen = new AtomicInteger(new Random().nextInt(1000) + 1000)

  case class JobItem(jobId: Int = jobIdGen.incrementAndGet(), startTime: Long, task: Future[_])

}
