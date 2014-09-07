package com.ldaniels528.verify

import java.io.File.separator
import java.io.{File, FileInputStream}
import java.util.concurrent.atomic.AtomicInteger
import java.util.{Properties, Random}

import com.ldaniels528.tabular.Tabular
import com.ldaniels528.verify.VxRuntimeContext.JobItem
import com.ldaniels528.verify.io.EndPoint
import com.ldaniels528.verify.modules.avro.{AvroModule, AvroTables}
import com.ldaniels528.verify.modules.core.CoreModule
import com.ldaniels528.verify.modules.kafka.KafkaModule
import com.ldaniels528.verify.modules.storm.StormModule
import com.ldaniels528.verify.modules.zookeeper.{ZKProxy, ZookeeperModule}
import com.ldaniels528.verify.modules.{CommandParser, Command, ModuleManager}
import com.ldaniels528.verify.util.BinaryMessaging
import com.ldaniels528.verify.vscript.{RootScope, VScriptCompiler}
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Properties.userHome
import scala.util.Try

/**
 * Verify Runtime Context
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
case class VxRuntimeContext(zkHost: String, zkPort: Int) extends BinaryMessaging {
  private val logger = LoggerFactory.getLogger(getClass)

  // capture standard output
  val out = System.out
  val err = System.err

  // create the root-level scope
  implicit val scope = RootScope()

  // get the ZooKeeper end-point
  val zkEndPoint = EndPoint(zkHost, zkPort)
  val remoteHost = zkEndPoint.toString

  // the default state of the application is "alive"
  var alive = true

  // define the history properties
  var historyFile = new File(s"$userHome$separator.verify${separator}history.txt")

  // define the configuration file & properties
  val configFile = new File(s"$userHome$separator.verify${separator}config.properties")
  val configProps = loadConfiguration(configFile)

  // create the ZooKeeper proxy
  val zkProxy = ZKProxy(zkEndPoint)

  // define the job stack
  val jobs = mutable.Buffer[JobItem]()

  // create the module manager
  val moduleManager = new ModuleManager(scope)

  // create the result handler
  val resultHandler = new VxResultHandler(this)

  // load the built-in modules
  moduleManager ++= Seq(
    new AvroModule(this),
    new CoreModule(this),
    new KafkaModule(this),
    new StormModule(this),
    new ZookeeperModule(this))

  // set the zookeeper module as the "active" module
  moduleManager.findModuleByName("zookeeper") foreach { module =>
    moduleManager.setActiveModule(module)
  }

  // various shared state variables
  def autoSwitching: Boolean = scope.getValue[Boolean]("autoSwitching") getOrElse false

  def autoSwitching_=(enabled: Boolean) = scope.setValue("autoSwitching", Option(enabled))

  // the number of columns to display when displaying bytes
  def columns: Int = scope.getValue[Int]("columns") getOrElse 25

  def columns_=(width: Int) = scope.setValue("columns", Option(width))

  def debugOn: Boolean = scope.getValue[Boolean]("debugOn") getOrElse false

  def debugOn_=(enabled: Boolean) = scope.setValue("debugOn", Option(enabled))

  def encoding: String = scope.getValue[String]("encoding") getOrElse "UTF-8"

  def encoding_=(charSet: String) = scope.setValue("encoding", Option(charSet))

  // ZooKeeper current working directory
  def zkCwd: String = scope.getValue[String]("zkCwd") getOrElse "/"

  def zkCwd_=(path: String) = scope.setValue("zkCwd", Option(path))

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

  def interpret(input: String): Try[Any] = {
    if (input.startsWith("#")) interpretVScript(input.tail) else interpretCommandLine(input)
  }

  /**
   * Interprets command line input
   * @param input the given line of input
   * @return a try-monad wrapped result
   */
  private def interpretCommandLine(input: String): Try[Any] = {
    // parse & evaluate the user input
    Try(parseInput(input) match {
      case Some((cmd, args)) =>
        // match the command
        val commandSet = moduleManager.commandSet
        commandSet.get(cmd) match {
          case Some(command) =>
            // verify and execute the command
            command.params.checkArgs(command, args)
            val result = command.fx(args)

            // auto-switch modules?
            if (autoSwitching && (command.promptAware || command.module.moduleName != "core")) {
              moduleManager.setActiveModule(command.module)
            }
            result
          case _ =>
            throw new IllegalArgumentException(s"'$input' not recognized")
        }
      case _ =>
    })
  }

  /**
   * Interprets VScript input
   * @param line the given line of input
   * @return a try-monad wrapped result
   */
  private def interpretVScript(line: String): Try[Any] = Try {
    val opCode = VScriptCompiler.compile(line, debugOn)
    if (debugOn) {
      logger.info(s"opCode = $opCode (${opCode.getClass.getName}})")
    }
    opCode.eval
  }

  def handleResult(result: Any)(implicit ec: ExecutionContext) = resultHandler.handleResult(result)

  /**
   * Parses a line of input into a tuple consisting of the command and its arguments
   * @param input the given line of input
   * @return an option of a tuple consisting of the command and its arguments
   */
  private def parseInput(input: String): Option[(String, Seq[String])] = {
    // parse the user input
    val pcs = CommandParser.parse(input)

    // return the command and arguments
    for {
      cmd <- pcs.headOption map (_.toLowerCase)
      args = pcs.tail
    } yield (cmd, args)
  }

}

/**
 * Verify Runtime Context Companion Object
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
object VxRuntimeContext {
  private var jobIdGen = new AtomicInteger(new Random().nextInt(1000) + 1000)

  case class JobItem(jobId: Int = jobIdGen.incrementAndGet(), startTime: Long, task: Future[_])

}