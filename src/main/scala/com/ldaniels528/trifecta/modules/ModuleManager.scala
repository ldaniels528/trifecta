package com.ldaniels528.trifecta.modules

import com.ldaniels528.trifecta.TxRuntimeContext
import com.ldaniels528.trifecta.command.Command
import com.ldaniels528.trifecta.modules.ModuleManager.ModuleVariable
import com.ldaniels528.trifecta.util.TxUtils._
import com.ldaniels528.trifecta.vscript.{Scope, Variable}

/**
 * Module Manager
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class ModuleManager(scope: Scope)(implicit rt: TxRuntimeContext) {
  private var commands = Map[String, Command]()
  private var variables = Seq[ModuleVariable]()
  private var moduleSet = Set[Module]()
  private var currentModule: Option[Module] = None

  /**
   * Adds a module to this manager
   * @param module the given module
   * @return the module manager instance
   */
  def +=(module: Module) = {
    moduleSet += module

    // reset the commands & variables collections
    updateCollections()
    this
  }

  /**
   * Adds a module to this manager
   * @param modules the given collection of module
   * @return the module manager instance
   */
  def ++=(modules: Seq[Module]) = {
    moduleSet ++= modules

    // reset the commands & variables collections
    updateCollections()
    this
  }

  def activeModule: Option[Module] = {
    currentModule ?? moduleSet.find(_.moduleName == "zookeeper") ?? moduleSet.headOption
  }

  def activeModule_=(module: Module) = currentModule = Option(module)

  /**
   * Returns a mapping of commands
   * @return a mapping of command name to command instance
   */
  def commandSet = commands

  /**
   * Returns a collection of modules
   * @return a collection of modules
   */
  def modules: Seq[Module] = moduleSet.toSeq

  /**
   * Returns a collection of module variables
   * @return a collection of module variables
   */
  def variableSet = variables

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
  def shutdown(): Unit = moduleSet.foreach(_.shutdown())

  private def updateCollections(): Unit = {
    // update the command collection
    commands = Map(moduleSet.toSeq flatMap (_.getCommands map (c => (c.name, c))): _*)

    // update the variable collection
    variables = moduleSet.toSeq flatMap { m =>
      m.getVariables map (v => ModuleVariable(m.moduleName, v))
    }

    // put the variables in the scope
    variables foreach (scope += _.variable)
  }

}

/**
 * Module Manager
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
object ModuleManager {

  case class ModuleVariable(moduleName: String, variable: Variable)

}