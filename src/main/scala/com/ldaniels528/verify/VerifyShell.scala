package com.ldaniels528.verify

import java.io.{ByteArrayOutputStream, PrintStream}

import com.ldaniels528.tabular.Tabular
import com.ldaniels528.verify.VerifyShell._
import com.ldaniels528.verify.io.avro._
import com.ldaniels528.verify.modules.Command
import org.fusesource.jansi.Ansi.Color._
import org.fusesource.jansi.Ansi._

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
  val out: PrintStream = rt.out
  val err: PrintStream = rt.err
  val buffer: ByteArrayOutputStream = rt.buffer

  // load the history, then schedule session history file updates
  SessionManagement.history.load(rt.historyFile)
  SessionManagement.setupHistoryUpdates(rt.historyFile, 60 seconds)

  // load the commands from the modules
  private def commandSet: Map[String, Command] = rt.moduleManager.commandSet

  // make sure we shutdown the ZooKeeper connection
  Runtime.getRuntime.addShutdownHook(new Thread {
    override def run() {
      // shutdown the ZooKeeper instance
      rt.zkProxy.close()

      // close each module
      rt.moduleManager.shutdown()
    }
  })

  /**
   * Interactive shell
   */
  def shell() {
    import jline.console.ConsoleReader
    VxConsole.wrap {
      out.println(ansi().fg(WHITE).a("Type '").fg(CYAN).a("help").fg(WHITE).a("' (or '").fg(CYAN).a("?").fg(WHITE).a("') to see the list of available commands").reset())
    }

    // display the state variables
    rt.states()

    // define the console reader
    val console = new ConsoleReader()

    do {
      // display the prompt, and get the next line of input
      val module = rt.moduleManager.activeModule getOrElse rt.moduleManager.modules.values.head

      // read a line from the console
      Option(console.readLine("%s@%s> ".format(module.name, module.prompt))) map (_.trim) foreach { line =>
        if (line.nonEmpty) {
          interpret(rt, commandSet, line) match {
            case Success(result) =>
              handleResult(result)(out)
              if (line != "history" && !line.startsWith("!") && !line.startsWith("?")) SessionManagement.history += line
            case Failure(e: IllegalArgumentException) =>
              if (rt.debugOn) e.printStackTrace()
              err.println(s"Syntax error: ${e.getMessage}")
            case Failure(e) =>
              if (rt.debugOn) e.printStackTrace()
              err.println(s"Runtime error: ${e.getMessage}")
          }
        }
      }
    } while (rt.alive)
  }

}

/**
 * Verify Console Shell Singleton
 * @author lawrence.daniels@gmail.com
 */
object VerifyShell {
  val VERSION = "0.1.1"

  // create the table generator
  private val tabular = new Tabular() with AvroTables

  /**
   * Application entry point
   * @param args the given command line arguments
   */
  def main(args: Array[String]) {
    // install the ANSI console plugin and display the title line
    VxConsole.wrap {
      System.out.println(ansi().fg(RED).a("Ve").fg(GREEN).a("ri").fg(BLUE).a("fy").fg(WHITE).a(s" v$VERSION").reset())
    }

    // if arguments were not passed, stop.
    args.toList match {
      case Nil => System.out.println("Usage: verify <zookeeperHost>")
      case params =>
        // were host and port argument passed?
        val host: String = params.head
        val port: Int = if (params.length > 1) params(1).toInt else 2181

        // create the runtime context
        val rt = VerifyShellRuntime(host, port)

        // start the shell
        val console = new VerifyShell(rt)
        console.shell()
    }

    // make sure all threads die
    sys.exit(0)
  }

  def interpret(rt: VerifyShellRuntime, commandSet: Map[String, Command], input: String): Try[Any] = {
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
            if (rt.autoSwitching) {
              rt.moduleManager.setActiveModule(command.module)
            }
            result
          case _ =>
            throw new IllegalArgumentException(s"'$input' not recognized")
        }
      case _ =>
    })
  }

  def handleResult(result: Any)(implicit out: PrintStream) {
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