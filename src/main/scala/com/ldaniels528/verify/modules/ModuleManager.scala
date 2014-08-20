package com.ldaniels528.verify.modules

import com.ldaniels528.verify.modules.Module.Command
import org.slf4j.LoggerFactory

/**
 * Module Manager
 * @author lawrence.daniels@gmail.com
 */
class ModuleManager() {
  private val logger = LoggerFactory.getLogger(getClass)
  private var commands = Map[String, Command]()
  var modules = Map[String, Module]()
  var activeModule: Option[Module] = None

  /**
   * Adds a module to this manager
   * @param module the given module
   * @return the module manager instance
   */
  def +=(module: Module) = {
    modules += module.name -> module

    // reset the commands collection
    commands = Map(modules.values.toSeq flatMap (_.getCommands) map (c => (c.name, c)): _*)
    this
  }

  /**
   * Adds a module to this manager
   * @param moduleSet the given collection of module
   * @return the module manager instance
   */
  def ++=(moduleSet: Seq[Module]) = {
    // import the modules
    moduleSet.foreach(m => modules += m.name -> m)

    // reset the commands collection
    commands = Map(modules.values.toSeq flatMap (_.getCommands) map (c => (c.name, c)): _*)

    // if no active module is set,
    // set the active module ("zookeeper" by default)
    if (activeModule.isEmpty) {
      modules.values.find(_.name == "zookeeper") match {
        case Some(module) => Some(module)
        case None => modules.headOption
      }
    }
    this
  }

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
    modules.values.find(_.name == name)
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