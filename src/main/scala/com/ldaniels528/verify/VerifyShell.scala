package com.ldaniels528.verify

import java.io.PrintStream

import com.ldaniels528.verify.VxConsole._
import com.ldaniels528.verify.modules.Command
import org.fusesource.jansi.Ansi.Color._

import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.{Failure, Success}

/**
 * Verify Console Shell Application
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class VerifyShell(rt: VxRuntimeContext) {
  implicit val scope = rt.scope

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
      // display the welcome message
      out.println(a"${WHITE}Type '${CYAN}help$WHITE' (or '$CYAN?$WHITE') to see the list of available commands")

      // display the state variables
      for (mv <- rt.moduleManager.variableSet) {
        val (value, color) = mv.variable.eval match {
          case Some(v: Boolean) => if (v) ("On", GREEN) else ("Off", YELLOW)
          case Some(v: String) => (v, CYAN)
          case Some(v) => (v.toString, MAGENTA)
          case None => ("(undefined)", RED)
        }
        val module = mv.moduleName
        val name = mv.variable.name
        out.println(a"$WHITE[*] $CYAN$module: $WHITE$name is $color$value")
      }
    }

    // define the console reader
    val consoleReader = new ConsoleReader()
    consoleReader.setExpandEvents(false)

    do {
      // display the prompt, and get the next line of input
      val module = rt.moduleManager.activeModule getOrElse rt.moduleManager.modules.head

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

  /**
   * Application entry point
   * @param args the given command line arguments
   */
  def main(args: Array[String]) {
    import org.fusesource.jansi.Ansi.Color._

    // use the ANSI console plugin to display the title line
    vxAnsi {
      System.out.println(a"${RED}Ve${GREEN}ri${CYAN}fy ${YELLOW}v$VERSION")
    }

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