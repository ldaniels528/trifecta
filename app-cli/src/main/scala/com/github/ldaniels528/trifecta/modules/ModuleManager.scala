package com.github.ldaniels528.trifecta.modules

import com.github.ldaniels528.commons.helpers.OptionHelper._
import com.github.ldaniels528.trifecta.TxRuntimeContext
import com.github.ldaniels528.trifecta.command.Command
import org.slf4j.LoggerFactory

/**
  * Module Manager
  * @author lawrence.daniels@gmail.com
  */
class ModuleManager()(implicit rt: TxRuntimeContext) {
  private val logger = LoggerFactory.getLogger(getClass)
  private var moduleSet = Set[Module]()
  private var currentModule: Option[Module] = None

  /**
    * Adds a module to this manager
    * @param module the given module
    */
  def +=(module: Module) = moduleSet += module

  /**
    * Adds a module to this manager
    * @param modules the given collection of module
    */
  def ++=(modules: Seq[Module]) = moduleSet ++= modules

  /**
    * Returns the active module
    * @return the [[Module module]]
    */
  def activeModule: Option[Module] = {
    currentModule ?? moduleSet.find(_.moduleName == "core") ?? moduleSet.headOption
  }

  /**
    * Sets the active module
    * @param module the [[Module module]]
    */
  def activeModule_=(module: Module) = currentModule = Option(module)

  /**
    * Returns a mapping of commands
    * @return a mapping of command name to command instance
    */
  def commandSet = Map(modules flatMap (_.getCommands map (c => (c.name, c))): _*)

  /**
    * Returns a collection of modules
    * @return a collection of modules
    */
  def modules: Seq[Module] = moduleSet.toSeq

  /**
    * Retrieves a command by name
    * @param name the name of the desired module
    * @return an option of a command
    */
  def findCommandByName(name: String): Option[Command] = commandSet.get(name.toLowerCase)

  /**
    * Retrieves a module by name
    * @param name the name of the desired module
    * @return an option of a module
    */
  def findModuleByName(name: String): Option[Module] = moduleSet.find(_.moduleName == name)

  /**
    * Retrieves a module by prefix
    * @param prefix the prefix of the desired module
    * @return an option of a module
    */
  def findModuleByPrefix(prefix: String): Option[Module] = moduleSet.find(_.supportedPrefixes.contains(prefix))

  /**
    * Sets the active module
    * @param module the given module
    */
  def setActiveModule(module: Module): Unit = currentModule = Option(module)

  /**
    * Shuts down all modules
    */
  def shutdown() = moduleSet.foreach { module =>
    logger.info(s"Module '${module.moduleName}' is shutting down...")
    module.shutdown()
    logger.info(s"Module '${module.moduleName}' shutdown complete")
  }

}
