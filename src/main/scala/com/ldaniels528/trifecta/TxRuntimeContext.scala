package com.ldaniels528.trifecta

import com.ldaniels528.trifecta.command.CommandParser
import com.ldaniels528.trifecta.modules.ModuleManager
import com.ldaniels528.trifecta.modules.core.CoreModule
import com.ldaniels528.trifecta.modules.kafka.KafkaModule
import com.ldaniels528.trifecta.modules.storm.StormModule
import com.ldaniels528.trifecta.modules.zookeeper.ZookeeperModule
import com.ldaniels528.trifecta.support.zookeeper.ZKProxy
import com.ldaniels528.trifecta.vscript.VScriptCompiler
import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContext
import scala.util.Try

/**
 * Trifecta Runtime Context
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class TxRuntimeContext(val config: TxConfig, val zkProxy: ZKProxy) {
  private val logger = LoggerFactory.getLogger(getClass)
  private implicit val scope = config.scope

  // create the result handler
  private val resultHandler = new TxResultHandler(config)

  // create the module manager
  val moduleManager = new ModuleManager(config.scope)

  // load the built-in modules
  moduleManager ++= Seq(
    new CoreModule(this),
    new KafkaModule(this),
    new StormModule(this),
    new ZookeeperModule(this))

  // set the zookeeper module as the "active" module
  moduleManager.findModuleByName("zookeeper") foreach { module =>
    moduleManager.setActiveModule(module)
  }

  def handleResult(result: Any)(implicit ec: ExecutionContext) = resultHandler.handleResult(result)

  def interpret(input: String): Try[Any] = {
    if (input.startsWith("#")) interpretVScript(input.tail) else interpretCommandLine(input)
  }

  def shutdown(): Unit = zkProxy.close()

  /**
   * Interprets command line input
   * @param input the given line of input
   * @return a try-monad wrapped result
   */
  private def interpretCommandLine(input: String): Try[Any] = Try {
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
