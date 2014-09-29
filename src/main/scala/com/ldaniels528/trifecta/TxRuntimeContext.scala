package com.ldaniels528.trifecta

import com.ldaniels528.trifecta.command.CommandParser
import com.ldaniels528.trifecta.modules.ModuleManager
import com.ldaniels528.trifecta.modules.core.CoreModule
import com.ldaniels528.trifecta.modules.elasticSearch.ElasticSearchModule
import com.ldaniels528.trifecta.modules.kafka.KafkaModule
import com.ldaniels528.trifecta.modules.storm.StormModule
import com.ldaniels528.trifecta.modules.zookeeper.ZookeeperModule
import com.ldaniels528.trifecta.support.io.OutputHandler
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
    new StormModule(config),
    new ZookeeperModule(config))

  // set the zookeeper module as the "active" module
  moduleManager.findModuleByName("zookeeper") foreach { module =>
    moduleManager.setActiveModule(module)
  }

  /**
   * Returns the output handler for the given output URL
   * @param url the given output URL (e.g. "es:/quotes/quote/GDF")
   */
  def getOutputHandler(url: String): Option[OutputHandler] = {
    // get just the prefix
    val (prefix, _) = {
      val index = url.indexOf(':')
      if (index == -1)
        throw new IllegalArgumentException(s"Malformed output URL: $url")
      url.splitAt(index)
    }

    // locate the module
    moduleManager.findModuleByPrefix(prefix) flatMap (_.getOutputHandler(url))
  }

  def handleResult(result: Any, input: String)(implicit ec: ExecutionContext) = {
    resultHandler.handleResult(result, input)
  }

  def interpret(input: String): Try[Any] = {
    if (input.startsWith("#")) interpretVScript(input.tail) else interpretCommandLine(input)
  }

  def shutdown(): Unit = ()

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

}
