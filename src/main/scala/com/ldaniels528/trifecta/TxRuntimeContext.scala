package com.ldaniels528.trifecta

import com.ldaniels528.trifecta.command.CommandParser
import com.ldaniels528.trifecta.modules.ModuleManager
import com.ldaniels528.trifecta.modules.core.CoreModule
import com.ldaniels528.trifecta.modules.elasticSearch.ElasticSearchModule
import com.ldaniels528.trifecta.modules.kafka.KafkaModule
import com.ldaniels528.trifecta.modules.mongodb.MongoModule
import com.ldaniels528.trifecta.modules.storm.StormModule
import com.ldaniels528.trifecta.modules.zookeeper.ZookeeperModule
import com.ldaniels528.trifecta.support.io.{InputSource, OutputSource}
import com.ldaniels528.trifecta.vscript.VScriptCompiler
import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContext
import scala.util.Try

/**
 * Trifecta Runtime Context
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
case class TxRuntimeContext(config: TxConfig) {
  private[trifecta] val logger = LoggerFactory.getLogger(getClass)
  private implicit val scope = config.scope

  // create the result handler
  private val resultHandler = new TxResultHandler(config)

  // create the module manager
  val moduleManager = new ModuleManager(scope)(this)

  // load the built-in modules
  moduleManager ++= Seq(
    new CoreModule(config),
    new ElasticSearchModule(config),
    new KafkaModule(config),
    new MongoModule(config),
    new StormModule(config),
    new ZookeeperModule(config))

  // set the "active" module
  moduleManager.findModuleByName("core") foreach moduleManager.setActiveModule

  /**
   * Returns the input handler for the given output URL
   * @param url the given input URL (e.g. "es:/quotes/quote/GDF")
   * @return an option of an [[InputSource]]
   */
  def getInputHandler(url: String): Option[InputSource] = {
    // get just the prefix
    val (prefix, _) = parseSourceURL(url) getOrElse die(s"Malformed input source URL: $url")

    // locate the module
    moduleManager.findModuleByPrefix(prefix) flatMap (_.getInputSource(url))
  }

  /**
   * Returns the output handler for the given output URL
   * @param url the given output URL (e.g. "es:/quotes/$exchange/$symbol")
   * @return an option of an [[OutputSource]]
   */
  def getOutputHandler(url: String): Option[OutputSource] = {
    // get just the prefix
    val (prefix, _) = parseSourceURL(url) getOrElse die(s"Malformed output source URL: $url")

    // locate the module
    moduleManager.findModuleByPrefix(prefix) flatMap (_.getOutputSource(url))
  }

  def handleResult(result: Any, input: String)(implicit ec: ExecutionContext) = {
    resultHandler.handleResult(result, input)
  }

  def interpret(input: String): Try[Any] = {
    input match {
      case s if s.startsWith("`") && s.endsWith("`") => executeCommand(s.drop(1).dropRight(1))
      case s if s.startsWith("#") => interpretVScript(input.tail)
      case s => interpretCommandLine(s)
    }
  }

  def shutdown(): Unit = moduleManager.shutdown()

  private def die[S](message: String): S = throw new IllegalArgumentException(message)

  /**
   * Executes a local system command
   * @example `ps -ef`
   */
  private def executeCommand(command: String): Try[String] = {
    import scala.sys.process._

    Try(command.!!)
  }

  /**
   * Interprets command line input
   * @param input the given line of input
   * @return a try-monad wrapped result
   */
  private def interpretCommandLine(input: String): Try[Any] = Try {
    implicit val rtc = this

    // parse the input into tokens
    val tokens = CommandParser.parseTokens(input)

    // convert the tokens into Unix-style arguments
    val unixArgs = CommandParser.parseUnixLikeArgs(tokens)

    // match the command
    val commandSet = moduleManager.commandSet

    for {
      commandName <- unixArgs.commandName
      command = commandSet.getOrElse(commandName, throw new IllegalArgumentException(s"command '$commandName' not found"))

    } yield {
      // trifecta and execute the command
      command.params.checkArgs(command, tokens)
      val result = command.fx(unixArgs)

      // auto-switch modules?
      if (config.autoSwitching && (command.promptAware || command.module.moduleName != "core")) {
        moduleManager.setActiveModule(command.module)
      }
      result
    }
  }

  /**
   * Interprets VScript input
   * @param line the given line of input
   * @return a try-monad wrapped result
   */
  private def interpretVScript(line: String): Try[Any] = Try {
    val opCode = VScriptCompiler.compile(line, config.debugOn)
    if (config.debugOn) {
      logger.info(s"opCode = $opCode (${opCode.getClass.getName}})")
    }
    opCode.eval
  }

  /**
   * Parses the the prefix and path from the I/O source URL
   * @param url the I/O source URL
   * @return the tuple represents the prefix and path
   */
  private def parseSourceURL(url: String): Option[(String, String)] = {
    val index = url.indexOf(':')
    if (index == -1) None else Option(url.splitAt(index))
  }

}
