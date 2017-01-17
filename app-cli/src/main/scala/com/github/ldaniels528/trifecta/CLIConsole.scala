package com.github.ldaniels528.trifecta

import java.io.{File, PrintStream}

import com.github.ldaniels528.commons.helpers.OptionHelper._
import com.github.ldaniels528.trifecta.command.CommandParser
import com.github.ldaniels528.trifecta.io.{AsyncIO, IOCounter}
import com.github.ldaniels528.trifecta.messages.MessageSourceFactory
import com.github.ldaniels528.trifecta.messages.query.parser.KafkaQueryParser
import com.github.ldaniels528.trifecta.modules.ModuleHelper.die
import com.github.ldaniels528.trifecta.modules.{Module, ModuleManager}
import org.apache.zookeeper.KeeperException.ConnectionLossException

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{ExecutionContext, Future}
import scala.io.Source
import scala.language.postfixOps
import scala.tools.jline.console.ConsoleReader
import scala.tools.jline.console.history.FileHistory
import scala.util.{Failure, Success, Try}

/**
  * CLI Console
  * @author lawrence.daniels@gmail.com
  */
class CLIConsole(rt: TxRuntimeContext,
                 jobManager: JobManager,
                 messageSourceFactory: MessageSourceFactory,
                 moduleManager: ModuleManager,
                 resultHandler: TxResultHandler) {
  private val config = rt.config

  // redirect standard output
  val out: PrintStream = config.out
  val err: PrintStream = config.err

  // initialize the custom modules
  Try {
    val libsDirectory = config.libsDirectory
    if (libsDirectory.isDirectory) {
      val modulesFile = new File(TxConfig.trifectaPrefs, "modules.json")
      if (modulesFile.exists()) {
        // create our custom-class classloader
        val jfCL = config.createUrlClassLoader(getClass.getClassLoader, libsDirectory)

        // load the user defined modules
        val modules = Module.loadUserDefinedModules(config, modulesFile, jfCL)
        moduleManager ++= modules
      }
    }
  }

  /**
    * Executes the given command line expression
    * @param line the given command line expression
    */
  def execute(line: String) {
    if(config.debugOn) {
      out.println(s"input: |$line|")
    }

    interpret(line) match {
      case Success(result) =>
        // if debug is enabled, display the object value and class name
        if (config.debugOn) showDebug(result)

        // handle the result
        handleResult(result, line)
      case Failure(e: ConnectionLossException) =>
        err.println("Zookeeper connection loss error - use 'zconnect' to re-establish a connection")
      case Failure(e: IllegalArgumentException) =>
        if (rt.config.debugOn) e.printStackTrace()
        err.println(s"Syntax error: ${getErrorMessage(e)}")
      case Failure(e) =>
        if (rt.config.debugOn) e.printStackTrace()
        err.println(s"Runtime error: ${getErrorMessage(e)}")
    }
  }

  def executeScript(scriptFile: File) {
    Source.fromFile(scriptFile).getLines() foreach { line =>
      execute(line)
    }
  }

  /**
    * Interactive shell
    */
  def shell() {
    // use the ANSI console plugin to display the title line
    out.println("Type 'help' (or ?) to see the list of available commands")

    if (rt.config.autoSwitching) {
      out.println("Module Auto-Switching is On")
    }

    // define the console reader
    val consoleReader = new ConsoleReader()
    consoleReader.setExpandEvents(false)
    val history = new FileHistory(TxConfig.historyFile)
    consoleReader.setHistory(history)

    do {
      // display the prompt, and get the next line of input
      val module = moduleManager.activeModule getOrElse moduleManager.modules.head

      // read a line from the console
      Try {
        Option(consoleReader.readLine("%s:%s> ".format(module.moduleLabel, module.prompt))) map (_.trim) foreach { line =>
          if (line.nonEmpty) {
            interpret(line) match {
              case Success(result) =>
                // if debug is enabled, display the object value and class name
                if (config.debugOn) showDebug(result)

                // handle the result
                handleResult(result, line)
              case Failure(e: ConnectionLossException) =>
                err.println("Zookeeper connect loss error - use 'zconnect' to re-establish a connection")
              case Failure(e: IllegalArgumentException) =>
                if (rt.config.debugOn) e.printStackTrace()
                err.println(s"Syntax error: ${getErrorMessage(e)}")
              case Failure(e) =>
                if (rt.config.debugOn) e.printStackTrace()
                err.println(s"Runtime error: ${getErrorMessage(e)}")
            }
          }
        }
      }
    } while (config.isAlive)

    // flush the console
    consoleReader.flush()
    consoleReader.setExpandEvents(false)

    // shutdown all resources
    moduleManager.shutdown()
  }

  /**
    * Executes a local system command
    * @example `ps -ef`
    */
  private def runLocalCommand(command: String): Try[String] = {
    import scala.sys.process._

    Try(command.!!)
  }

  private def handleResult(value: Any, input: String)(implicit ec: ExecutionContext) {
    val result = moduleManager.modules.foldLeft[Option[AnyRef]](None) { (result, module) =>
      result ?? module.decipher(value)
    }
    resultHandler.handleResult(result getOrElse value, input)
  }

  private def interpret(input: String): Try[Any] = {
    input.trim match {
      case s if s.startsWith("`") && s.endsWith("`") => runLocalCommand(s.drop(1).dropRight(1))
      case s => interpretCommandLine(s)
    }
  }

  /**
    * Interprets command line input
    * @param input the given line of input
    * @return a try-monad wrapped result
    */
  private def interpretCommandLine(input: String) = Try {
    // is the input a query?
    if (input.toLowerCase.startsWith("select")) {
      val counter = IOCounter(System.currentTimeMillis())
      AsyncIO(KafkaQueryParser(input).executeQuery(rt, counter), counter)
    }
    else {
      // parse the input into tokens
      val tokens = CommandParser.parseTokens(input)

      // convert the tokens into Unix-style arguments
      val unixArgs = CommandParser.parseUnixLikeArgs(tokens)

      // match the command
      val commandSet = moduleManager.commandSet

      for {
        commandName <- unixArgs.commandName
        command = commandSet.getOrElse(commandName, die(s"command '$commandName' not found"))

      } yield {
        // verify and execute the command
        command.params.checkArgs(command, tokens)
        val result = command.fx(unixArgs)

        // auto-switch modules?
        if (config.autoSwitching && (command.promptAware || command.module.moduleName != "core")) {
          moduleManager.setActiveModule(command.module)
        }
        result
      }
    }
  }

  private def showDebug(result: Any): Unit = {
    result match {
      case Failure(e) => e.printStackTrace(out)
      case AsyncIO(task, _) => showDebug(task.value)
      case f: Future[_] => showDebug(f.value)
      case Some(v) => showDebug(v)
      case Success(v) => showDebug(v)
      case _ => out.println(s"result: $result ${Option(result) map (_.getClass.getName) getOrElse ""}")
    }
  }

  private def getErrorMessage(t: Throwable): String = {
    Option(t.getMessage) getOrElse t.toString
  }

}