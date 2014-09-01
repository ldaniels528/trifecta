package com.ldaniels528.verify

import java.io.File.separator
import java.io.{File, FileInputStream}
import java.util.Properties

import com.ldaniels528.tabular.Tabular
import com.ldaniels528.verify.io.EndPoint
import com.ldaniels528.verify.modules.avro.{AvroModule, AvroTables}
import com.ldaniels528.verify.modules.core.CoreModule
import com.ldaniels528.verify.modules.kafka.KafkaModule
import com.ldaniels528.verify.modules.kafka.KafkaSubscriber.MessageData
import com.ldaniels528.verify.modules.storm.StormModule
import com.ldaniels528.verify.modules.zookeeper.{ZKProxy, ZookeeperModule}
import com.ldaniels528.verify.modules.{Command, ModuleManager}
import com.ldaniels528.verify.util.BinaryMessaging
import com.ldaniels528.verify.vscript.{RootScope, VScriptCompiler}
import org.slf4j.LoggerFactory

import scala.collection.GenTraversableOnce
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.Properties.userHome
import scala.util.{Failure, Success, Try}

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

  // define the tabular instance
  val tabular = new Tabular() with AvroTables

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

  // create the module manager
  val moduleManager = new ModuleManager(scope)

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
    interpretLegacy(input) /*match {
      case s1 @ Success(v) => s1
      case f1 @ Failure(e1) =>
        interpretVScript(input) match {
          case s2 @ Success(v) => s2
          case Failure(_) => f1
        }
    } */
  }

  def interpretLegacy(input: String): Try[Any] = {
    // parse & evaluate the user input
    Try(parseInput(input) match {
      case Some((cmd, args)) =>
        // match the command
        val commandSet = moduleManager.commandSet
        commandSet.get(cmd) match {
          case Some(command) =>
            // verify and execute the command
            checkArgs(command, args)
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

  def interpretVScript(line:String): Try[Any] = Try {
    val opCode = VScriptCompiler.compile(line, debugOn)
    if(debugOn) {
      logger.info(s"opCode = $opCode (${opCode.getClass.getName}})")
    }
    opCode.eval
  }

  def handleResult(result: Any) {
    result match {
      // handle binary data
      case message: Array[Byte] if message.isEmpty => out.println("No data returned")
      case message: Array[Byte] => dumpMessage(message)(this, out)
      case MessageData(offset, _, _, message) => dumpMessage(offset, message)(this, out)

      // handle Either cases
      case e: Either[_, _] => e match {
        case Left(v) => handleResult(v)
        case Right(v) => handleResult(v)
      }

      // handle Future cases
      case f: Future[_] => handleResult(Await.result(f, 60.seconds))

      // handle Option cases
      case o: Option[_] => o match {
        case Some(v) => handleResult(v)
        case None => out.println("No data returned")
      }

      // handle Try cases
      case t: Try[_] => t match {
        case Success(v) => handleResult(v)
        case Failure(e) => throw e
      }

      // handle lists and sequences of case classes
      case s: Seq[_] if s.isEmpty => out.println("No data returned")
      case s: Seq[_] if !Tabular.isPrimitives(s) => tabular.transform(s) foreach out.println

      // handle lists and sequences of primitives
      case g: GenTraversableOnce[_] => g foreach out.println

      // anything else ...
      case x => if (x != null && !x.isInstanceOf[Unit]) out.println(x)
    }
  }

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
