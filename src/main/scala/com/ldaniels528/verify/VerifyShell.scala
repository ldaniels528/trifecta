package com.ldaniels528.verify

import java.io.PrintStream

import com.ldaniels528.tabular.Tabular
import com.ldaniels528.verify.VxConsole._
import com.ldaniels528.verify.modules.Command
import com.ldaniels528.verify.modules.avro.AvroTables
import org.fusesource.jansi.Ansi.Color._
import org.fusesource.jansi.Ansi._

import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.{Failure, Success}

/**
 * Verify Console Shell Application
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class VerifyShell(rt: VxRuntimeContext) {
  // redirect standard output
  val out: PrintStream = rt.out
  val err: PrintStream = rt.err

  // load the history, then schedule session history file updates
  val history: History = SessionManagement.history
  history.load(rt.historyFile)
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

    // use the ANSI console plugin to display the title line
    vxAnsi {
      System.out.println(ansi().fg(RED).a("Ve").fg(GREEN).a("ri").fg(BLUE).a("fy").fg(WHITE).a(s" v${VerifyShell.VERSION}").reset())
    }

    // display the state variables
    rt.states()

    // define the console reader
    val consoleReader = new ConsoleReader()
    consoleReader.setExpandEvents(false)

    do {
      // display the prompt, and get the next line of input
      val module = rt.moduleManager.activeModule getOrElse rt.moduleManager.modules.values.head

      // read a line from the console
      Option(consoleReader.readLine("%s@%s> ".format(module.moduleName, module.prompt))) map (_.trim) foreach { line =>
        if (line.nonEmpty) {
          rt.interpret(commandSet, line) match {
            case Success(result) =>
              rt.handleResult(result)
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
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
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
    // if arguments were not passed, stop.
    args.toList match {
      case Nil => System.out.println("Usage: verify <zookeeperHost>")
      case params =>
        // were host and port argument passed?
        val host: String = params.head
        val port: Int = if (params.length > 1) params(1).toInt else 2181

        // create the runtime context
        val rt = VxRuntimeContext(host, port)

        // start the shell
        val console = new VerifyShell(rt)
        console.shell()
    }

    // make sure all threads die
    sys.exit(0)
  }

}