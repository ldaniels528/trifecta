package com.ldaniels528.trifecta

import java.io.PrintStream

import com.ldaniels528.trifecta.TxConsole._
import com.ldaniels528.trifecta.command.Command
import com.ldaniels528.trifecta.vscript.Scope
import org.apache.zookeeper.KeeperException.ConnectionLossException
import org.fusesource.jansi.Ansi.Color._

import scala.concurrent.ExecutionContext.Implicits._
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.{Failure, Success, Try}

/**
 * Trifecta Console Shell Application
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class TrifectaShell(config: TxConfig, rt: TxRuntimeContext) {
  private implicit val scope: Scope = config.scope

  // redirect standard output
  val out: PrintStream = rt.config.out
  val err: PrintStream = rt.config.err

  // load the history, then schedule session history file updates
  val history: History = SessionManagement.history
  history.load(config.historyFile)
  SessionManagement.setupHistoryUpdates(config.historyFile, 60 seconds)

  // load the commands from the modules
  private def commandSet: Map[String, Command] = rt.moduleManager.commandSet

  // make sure we shutdown the ZooKeeper connection
  Runtime.getRuntime.addShutdownHook(new Thread {
    override def run() {
      // shutdown the ZooKeeper instance
      rt.shutdown()

      // close each module
      rt.moduleManager.shutdown()
    }
  })

  /**
   * Interactive shell
   */
  def shell() {
    import jline.console.ConsoleReader

    // use the ANSI console plugin to display the title line
    vxAnsi {
      // display the welcome message
      out.println(a"${WHITE}Type '${CYAN}help$WHITE' (or '$CYAN?$WHITE') to see the list of available commands")
    }

    if (rt.config.autoSwitching) {
      out.println("Module Auto-Switching is On")
    }

    // define the console reader
    val consoleReader = new ConsoleReader()
    consoleReader.setExpandEvents(false)

    do {
      // display the prompt, and get the next line of input
      val module = rt.moduleManager.activeModule getOrElse rt.moduleManager.modules.head

      // read a line from the console
      Try {
        Option(consoleReader.readLine("%s:%s> ".format(module.moduleName, module.prompt))) map (_.trim) foreach { line =>
          if (line.nonEmpty) {
            rt.interpret(line) match {
              case Success(result) =>
                rt.handleResult(result, line)
                if (!ineligibleHistory(line)) SessionManagement.history += line
              case Failure(e: ConnectionLossException) =>
                err.println("Zookeeper connect loss error - use 'zreconnect' to re-establish a connection")
              case Failure(e: IllegalArgumentException) =>
                if (rt.config.debugOn) e.printStackTrace()
                err.println(s"Syntax error: ${e.getMessage}")
              case Failure(e) =>
                if (rt.config.debugOn) e.printStackTrace()
                err.println(s"Runtime error: ${getErrorMessage(e)}")
            }
          }
        }
      }
    } while (config.alive)
  }

  /**
   * Indicates whether the given line is ineligible for addition into the session history
   * @param line the given line of execution
   * @return true, if the line of execution is ineligible for addition into the session history
   */
  private def ineligibleHistory(line: String): Boolean = {
    line.startsWith("history") || line.startsWith("!") || SessionManagement.history.last.exists(_ == line)
  }

  private def getErrorMessage(t: Throwable): String = {
    Option(t.getMessage) getOrElse t.toString
  }

}

/**
 * Trifecta Console Shell Companion Object
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
object TrifectaShell {
  val VERSION = "0.1.6"

  /**
   * Application entry point
   * @param args the given command line arguments
   */
  def main(args: Array[String]) {
    import org.fusesource.jansi.Ansi.Color._

    // use the ANSI console plugin to display the title line
    vxAnsi {
      System.out.println(a"${RED}Tri${GREEN}fect${CYAN}a ${YELLOW}v$VERSION")
    }

    // if arguments were not passed, stop.
    args.toList match {
      case Nil => System.out.println("Usage: trifecta <zookeeperHost>")
      case params =>
        // were host and port argument passed?
        val host: String = params.head
        val port: Int = if (params.length > 1) params(1).toInt else 2181

        // load the configuration
        val config = new TxConfig(host, port)

        // create the runtime context
        val rt = new TxRuntimeContext(config)

        // start the shell
        val console = new TrifectaShell(config, rt)
        console.shell()
    }

    // make sure all threads die
    sys.exit(0)
  }

}