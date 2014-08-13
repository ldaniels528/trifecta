package com.ldaniels528.verify

import java.io.{ByteArrayOutputStream, PrintStream}

import com.ldaniels528.verify.io.avro._
import com.ldaniels528.verify.modules.Module
import com.ldaniels528.verify.modules.Module.Command
import com.ldaniels528.verify.modules.kafka._
import com.ldaniels528.verify.modules.unix.UnixModule
import com.ldaniels528.verify.modules.zookeeper._
import com.ldaniels528.verify.util.Tabular
import org.slf4j.LoggerFactory

import scala.collection.GenTraversableOnce
import scala.concurrent.duration._
import scala.io.Source
import scala.language.postfixOps
import scala.util.{Failure, Success, Try}

/**
 * Verify Console Shell Application
 * @author lawrence.daniels@gmail.com
 */
class VerifyShell(rt: VerifyShellRuntime) {
  // logger instance
  private val logger = LoggerFactory.getLogger(getClass)

  // define a custom tabular instance
  private val tabular = new Tabular() with AvroTables

  // redirect standard output and error to my own buffers
  private val out = System.out
  private val err = System.err
  private val buffer = new ByteArrayOutputStream(16384)
  System.setOut(new PrintStream(buffer))

  // session history
  private val history = new History(rt.maxHistory)
  history.load(rt.historyFile)

  // schedule session history file updates
  SessionManagement.setupHistoryUpdates(history, rt.historyFile, 5 minutes)

  // define the modules
  private val modules: Seq[Module] = Seq(
    new CoreModule(),
    new KafkaModule(rt, out),
    new UnixModule(rt, out),
    new ZookeeperModule(rt, out))

  // set the active module ("zookeeper" by default)
  private var activeModule: Module = modules.find(_.name == "zookeeper") getOrElse modules.head

  // load the commands from the modules
  private val commandSet: Map[String, Command] = loadModules(modules)

  // make sure we shutdown the ZooKeeper connection
  Runtime.getRuntime.addShutdownHook(new Thread {
    override def run() {
      // shutdown the ZooKeeper instance
      rt.zkProxy.close()

      // close each module
      modules.foreach(_.shutdown())
    }
  })

  /**
   * Interactive shell
   */
  def shell() {
    val userName = scala.util.Properties.userName

    do {
      // display the prompt, and get the next line of input
      out.print("%s@%s:%s> ".format(userName, rt.remoteHost, activeModule.prompt))
      val line = Console.readLine().trim

      if (line.nonEmpty) {
        interpret(line) match {
          case Success(result) =>
            handleResult(result)
            if (line != "history" && !line.startsWith("!")) history += line
          case Failure(e: IllegalArgumentException) =>
            if (rt.debugOn) e.printStackTrace()
            err.println(s"Syntax error: ${e.getMessage}")
          case Failure(e) =>
            if (rt.debugOn) e.printStackTrace()
            err.println(s"Runtime error: ${e.getMessage}")
        }
      }
    } while (rt.alive)
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

  private def handleResult(result: Any) {
    result match {
      // handle lists and sequences of case classes
      case s: Seq[_] if !Tabular.isPrimitives(s) => tabular.transform(s) foreach out.println

      // handle Either cases
      case e: Either[_, _] => e match {
        case Left(v) => handleResult(v)
        case Right(v) => handleResult(v)
      }

      // handle Option cases
      case o: Option[_] => o match {
        case Some(v) => handleResult(v)
        case None =>
      }

      // handle Try cases
      case t: Try[_] => t match {
        case Success(v) => handleResult(v)
        case Failure(e) => throw e
      }

      // handle lists and sequences of primitives
      case g: GenTraversableOnce[_] => g foreach out.println

      // anything else ...
      case x => if (x != null && !x.isInstanceOf[Unit]) out.println(x)
    }
  }

  private def interpret(input: String): Try[Any] = {
    // parse & evaluate the user input
    Try(parseInput(input) match {
      case Some((cmd, args)) =>
        // match the command
        commandSet.get(cmd) match {
          case Some(command) =>
            checkArgs(command, args)
            command.fx(args)
          case _ =>
            throw new IllegalArgumentException(s"'$input' not recognized")
        }
      case _ =>
    })
  }

  private def loadModules(modules: Seq[Module]): Map[String, Command] = {
    // gather all of the commands
    val commands = modules flatMap { module =>
      logger.info(s"Loading ${module.name} module...")
      module.getCommands
    }

    // return the command mapping
    Map(commands.map(c => c.name -> c): _*)
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

  /**
   * Executes a Java application via its "main" method
   * @param className the name of the class to invoke
   * @param args the arguments to pass to the application
   */
  private def runJava(className: String, args: String*): Iterator[String] = {
    // reset the buffer
    buffer.reset()

    // execute the command
    val commandClass = Class.forName(className)
    val mainMethod = commandClass.getMethod("main", classOf[Array[String]])
    mainMethod.invoke(null, args.toArray)

    // return the iteration of lines
    Source.fromBytes(buffer.toByteArray).getLines()
  }

  /**
   * Core Module
   * @author lawrence.daniels@gmail.com
   */
  class CoreModule() extends Module {

    val name = "core"

    override def prompt = history.size.toString

    val getCommands: Seq[Command] = Seq(
      Command(this, "!", executeHistory, (Seq("index"), Seq.empty), help = "Executes a previously issued command"),
      Command(this, "?", help, (Seq.empty, Seq("search-term")), help = "Provides the list of available commands"),
      Command(this, "charset", charSet, (Seq.empty, Seq("encoding")), help = "Retrieves or sets the character encoding"),
      Command(this, "class", inspectClass, (Seq.empty, Seq("action")), help = "Inspects a class using reflection"),
      Command(this, "debug", debug, (Seq.empty, Seq("state")), help = "Switches debugging on/off"),
      Command(this, "exit", exit, help = "Exits the shell"),
      Command(this, "help", help, help = "Provides the list of available commands"),
      Command(this, "history", listHistory, help = "Returns a list of previously issued commands"),
      Command(this, "modules", listModules, help = "Returns a list of previously issued commands"),
      Command(this, "resource", findResource, (Seq("resource-name"), Seq.empty), help = "Inspects the classpath for the given resource"),
      Command(this, "use", useModule, (Seq("module"), Seq.empty), help = "Switches the active module"))

    override def shutdown() = ()

    /**
     * Retrieves or sets the character encoding
     * Example: charset UTF-8
     * @param args the given arguments
     */
    def charSet(args: String*) = {
      args.headOption match {
        case Some(newEncoding) => rt.encoding = newEncoding
        case None => rt.encoding
      }
    }

    /**
     * Toggles the current debug state
     * @param args the given command line arguments
     * @return the current state ("On" or "Off")
     */
    def debug(args: String*): String = {
      if (args.isEmpty) rt.debugOn = !rt.debugOn else rt.debugOn = args.head.toBoolean
      s"debugging is ${if (rt.debugOn) "On" else "Off"}"
    }

    /**
     * Inspects the classpath for the given resource by name
     * Example: resource org/apache/http/message/BasicLineFormatter.class
     */
    def findResource(args: String*): String = {
      // get the class name (with slashes)
      val path = args.head
      val index = path.lastIndexOf('.')
      val resourceName = path.substring(0, index).replace('.', '/') + path.substring(index)
      logger.info(s"resource path is '$resourceName'")

      // determine the resource
      val classLoader = VerifyShell.getClass.getClassLoader
      val resource = classLoader.getResource(resourceName)
      String.valueOf(resource)
    }

    /**
     * "help" command - Provides the list of available commands
     */
    def help(args: String*): Seq[String] = {
      val list = commandSet.toSeq filter {
        case (name, cmd) => args.isEmpty || name.startsWith(args.head)
      } sortBy (_._1) map {
        case (name, cmd) =>
          CommandItem(name, cmd.module.name, cmd.help)
      }
      tabular.transform(list)
    }

    /**
     * Inspects a class using reflection
     * Example: class org.apache.commons.io.IOUtils -m
     */
    def inspectClass(args: String*): Seq[String] = {
      val className = extract(args, 0).getOrElse(getClass.getName).replace('/', '.')
      val action = extract(args, 1) getOrElse "-m"
      val beanClass = Class.forName(className)

      action match {
        case "-m" => beanClass.getDeclaredMethods map (_.toString)
        case "-f" => beanClass.getDeclaredFields map (_.toString)
        case _ => beanClass.getDeclaredMethods map (_.toString)
      }
    }

    /**
     * "!" command - History execution command. This command can either executed a
     * previously executed command by its unique identifier, or list (!?) all previously
     * executed commands.
     * Example 1: !123
     * Example 2: !?
     */
    def executeHistory(args: String*) = {
      for {
        index <- args.headOption
        command <- index match {
          case s if s == "?" => Some("history")
          case s if s == "!" => history.last
          case s if s.matches("\\d+") => history(index.toInt - 1)
          case _ => None
        }
      } {
        out.println(s">> $command")
        val result = interpret(command)
        handleResult(result)
      }
    }

    /**
     * "exit" command - Exits the shell
     */
    def exit(args: String*) = {
      rt.alive = false
      history.store(rt.historyFile)
    }

    def listHistory(args: String*): Seq[String] = {
      val lines = history.getLines
      val data = ((1 to lines.size) zip lines) map {
        case (itemNo, command) => HistoryItem(itemNo, command)
      }
      tabular.transform(data)
    }

    /**
     * "modules" command - Displays the list of modules
     * Example: modules
     * @param args the given command line arguments
     * @return
     */
    def listModules(args: String*): Seq[String] = {
      val list = modules.map(m => ModuleItem(m.name, m.getClass.getName, "loaded"))
      tabular.transform(list)
    }

    /**
     * "use" command - Switches the active module
     * Example: use kafka
     * @param args the given command line arguments
     */
    def useModule(args: String*) = {
      val moduleName = args.head
      modules.find(_.name == moduleName) match {
        case Some(module) => activeModule = module
        case None =>
          throw new IllegalArgumentException(s"Module '$moduleName' not found")
      }
    }

  }

  case class CommandItem(command: String, module: String, description: String)

  case class HistoryItem(itemNo: Int, command: String)

  case class ModuleItem(name: String, className: String, status: String)

}

/**
 * Verify Console Shell Singleton
 * @author lawrence.daniels@gmail.com
 */
object VerifyShell {
  val VERSION = "1.0.6"

  /**
   * Application entry point
   * @param args the given command line arguments
   */
  def main(args: Array[String]) {
    System.out.println(s"Verify Shell v$VERSION")

    // were host and port argument passed?
    val host: String = args.headOption getOrElse "localhost"
    val port: Int = if (args.length > 1) args(1).toInt else 2181

    // create the runtime context
    val rt = VerifyShellRuntime(host, port)

    // start the shell
    val console = new VerifyShell(rt)
    console.shell()

    // make sure all threads die
    sys.exit(0)
  }

}