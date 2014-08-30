package com.ldaniels528.verify

import VxRuntimeContext._
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
import com.ldaniels528.verify.vscript.RootScope

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
  // capture standard output
  val out = System.out
  val err = System.err

  // create the root-level scope
  val scope = RootScope()

  // define the tabular instance
  val tabular = new Tabular() with AvroTables

  // the Zookeeper remote host
  val remoteHost = s"$zkHost:$zkPort"

  // get the ZooKeeper end-point
  val zkEndPoint = EndPoint(zkHost, zkPort)

  // the default state of the application is "alive"
  var alive = true

  // various shared state variables
  var autoSwitching = false
  var debugOn = false
  var defaultFetchSize = 65536
  var encoding = "UTF-8"

  // ZooKeeper current working directory
  var zkCwd = "/"

  // the number of columns to display when displaying bytes
  var columns = 25

  // define the history properties
  var historyFile = new File(s"$userHome$separator.verify${separator}history.txt")

  // define the configuration file & properties
  val configFile = new File(s"$userHome$separator.verify${separator}config.properties")
  val configProps = loadConfiguration(configFile)

  // create the ZooKeeper proxy
  val zkProxy = ZKProxy(zkEndPoint)

  // create the module manager
  val moduleManager = new ModuleManager()

  // load the modules
  moduleManager ++= Seq(
    new AvroModule(this),
    new CoreModule(this),
    new KafkaModule(this),
    new StormModule(this),
    new ZookeeperModule(this))

  // set the zookeeper module as the "active" module
  moduleManager.findModuleByName("zookeeper") map { module =>
    moduleManager.setActiveModule(module)
  }

  /**
   * Displays the loaded configuration properties
   */
  def getStateMappings: Seq[StateMapping] = Seq(
    StateMapping("core", "encoding", encoding),
    StateMapping("core", "module auto-switching", autoSwitching),
    StateMapping("core", "debugging", debugOn),
    StateMapping("kafka", "fetch size", defaultFetchSize)
  )

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

  def interpret(commandSet: Map[String, Command], input: String): Try[Any] = {
    // parse & evaluate the user input
    Try(parseInput(input) match {
      case Some((cmd, args)) =>
        // match the command
        commandSet.get(cmd) match {
          case Some(command) =>
            // verify and execute the command
            checkArgs(command, args)
            val result = command.fx(args)

            // auto-switch modules?
            if (autoSwitching) {
              moduleManager.setActiveModule(command.module)
            }
            result
          case _ =>
            throw new IllegalArgumentException(s"'$input' not recognized")
        }
      case _ =>
    })
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
      case f: Future[_] => handleResult(Await.result(f, 30.seconds))

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

/**
 * Verify Runtime Context Singleton Object
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
case
object VxRuntimeContext {

  case class StateMapping(module: String, name: String, value: Any)

}
