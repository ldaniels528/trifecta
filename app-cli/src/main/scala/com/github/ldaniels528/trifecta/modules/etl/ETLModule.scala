package com.github.ldaniels528.trifecta.modules.etl

import java.io.File

import com.github.ldaniels528.trifecta.command.{Command, UnixLikeArgs, UnixLikeParams}
import com.github.ldaniels528.trifecta.io.{MessageInputSource, MessageOutputSource}
import com.github.ldaniels528.trifecta.modules.Module
import com.github.ldaniels528.trifecta.modules.etl.ETLModule.StoryInfo
import com.github.ldaniels528.trifecta.{TxConfig, TxRuntimeContext}

import scala.concurrent.ExecutionContext.Implicits.global

/**
  * Extract Transform Loading (ETL) Module
  * @author lawrence.daniels@gmail.com
  */
class ETLModule(config: TxConfig) extends Module {
  private val processor = new StoryProcessor()
  private var etlConfig_? : Option[StoryConfig] = None

  /**
    * Returns the commands that are bound to the module
    * @return the commands that are bound to the module
    */
  override def getCommands(implicit rt: TxRuntimeContext) = Seq(
    Command(this, "load", loadStory, UnixLikeParams(Seq("config" -> true)), help = "Load an ETL config into the current session"),
    Command(this, "run", run, UnixLikeParams(Seq("config" -> false)), help = "Executes an ETL config"),
    Command(this, "show", list, UnixLikeParams(), help = "Displays the currently loaded ETL config")
  )

  /**
    * Attempts to retrieve an input source for the given URL
    * @param url the given input URL
    * @return the option of an input source
    */
  override def getInputSource(url: String): Option[MessageInputSource] = None

  /**
    * Attempts to retrieve an output source for the given URL
    * @param url the given output URL
    * @return the option of an output source
    */
  override def getOutputSource(url: String): Option[MessageOutputSource] = None

  /**
    * Returns the name of the prefix (e.g. Seq("file"))
    * @return the name of the prefix
    */
  override def supportedPrefixes: Seq[String] = Nil

  /**
    * Returns the label of the module (e.g. "kafka")
    * @return the label of the module
    */
  override def moduleLabel = "etl"

  /**
    * Returns the name of the module (e.g. "kafka")
    * @return the name of the module
    */
  override def moduleName = "etl"

  /**
    * Returns the the information that is to be displayed while the module is active
    * @return the the information that is to be displayed while the module is active
    */
  override def prompt = etlConfig_?.map(_.id) getOrElse "***"

  /**
    * Called when the application is shutting down
    */
  override def shutdown(): Unit = {}

  /**
    * Displays the currently-loaded ETL configuration
    * @param params the given [[UnixLikeArgs arguments]]
    * @example show
    */
  def list(params: UnixLikeArgs) = {
    etlConfig_?.map(sc => StoryInfo(
      name = sc.id,
      archives = sc.archives.size,
      devices = sc.devices.size,
      filters = sc.filters.size,
      layouts = sc.layouts.size,
      properties = sc.properties.size,
      triggers = sc.triggers.size
    )).toList
  }

  /**
    * Loads an ETL configuration
    * @param params the given [[UnixLikeArgs arguments]]
    * @example load "./app-cli/src/test/resources/etl/eod_companies_kafka_json.xml"
    */
  def loadStory(params: UnixLikeArgs) = {
    params.args match {
      case path :: Nil =>
        val file = new File(path)
        etlConfig_? = processor.load(file).headOption
      case _ =>
        dieSyntax(params)
    }
  }

  /**
    * Runs an ETL configuration
    * @param params the given [[UnixLikeArgs arguments]]
    * @example run "./app-cli/src/test/resources/etl/eod_companies_csv.xml"
    */
  def run(params: UnixLikeArgs) = {
    params.args match {
      case path :: Nil =>
        val file = new File(path)
        etlConfig_? = processor.load(file).headOption
        etlConfig_? foreach processor.run
      case Nil if etlConfig_?.isDefined =>
        etlConfig_? foreach processor.run
      case _ =>
        die("No ETL config was specified or loaded")
    }
  }

}

/**
  * ETL Module Companion Object
  * @author lawrence.daniels@gmail.com
  */
object ETLModule {

  case class StoryInfo(name: String, archives: Int, devices: Int, filters: Int, layouts: Int, properties: Int, triggers: Int)

}