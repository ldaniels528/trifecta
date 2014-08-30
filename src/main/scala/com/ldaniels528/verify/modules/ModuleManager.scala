package com.ldaniels528.verify.modules

import com.ldaniels528.verify.modules.ModuleManager.ModuleVariable
import com.ldaniels528.verify.util.VxUtils._
import com.ldaniels528.verify.vscript.{Scope, Variable}

/**
 * Module Manager
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class ModuleManager(scope: Scope) {
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
   * Retrieves a module by name
   * @param name the name of the desired module
   * @return an option of a module
   */
  def findModuleByName(name: String): Option[Module] = {
    modules.values.find(_.moduleName == name)
  }

  /**
   * Sets the active module
   * @param module the given module
   */
  def setActiveModule(module: Module): Unit = {
    activeModule = Some(module)
  }

  /**
   * Shuts down all modules
   */
  def shutdown() {
    modules.values.foreach(_.shutdown())
  }

}