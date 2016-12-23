package com.github.ldaniels528.trifecta

import com.github.ldaniels528.trifecta.AppConstants._
import com.github.ldaniels528.trifecta.io.kafka.KafkaSandbox
import com.github.ldaniels528.trifecta.messages.{MessageReader, MessageSourceFactory, MessageWriter}
import com.github.ldaniels528.trifecta.modules.core.CoreModule
import com.github.ldaniels528.trifecta.modules.kafka.KafkaModule
import com.github.ldaniels528.trifecta.modules.zookeeper.ZookeeperModule
import com.github.ldaniels528.trifecta.modules.{Module, ModuleManager}
import com.github.ldaniels528.trifecta.CommandLineHelper._
import org.slf4j.LoggerFactory

import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.language.postfixOps
import scala.util.{Failure, Success, Try}

/**
  * Trifecta Shell (REPL)
  * @author lawrence.daniels@gmail.com
  */
object TrifectaShell {
  private val logger = LoggerFactory.getLogger(getClass)

  /**
    * Application entry point
    * @param args the given command line arguments
    */
  def main(args: Array[String]) {
    // interactive mode?
    val nonInteractiveMode = args.isNonInteractive
    if(!nonInteractiveMode) {
      println(s"Trifecta v$VERSION ($KAFKA_VERSION)")
    }

    // load the configuration
    val config = loadConfiguration(nonInteractiveMode)

    // startup the Kafka Sandbox?
    if (args.isKafkaSandbox) {
      initKafkaSandbox(config)
    }

    // create the dependencies
    val jobManager = new JobManager()
    val messageSourceFactory = new MessageSourceFactory()
    val rt = TxRuntimeContext(config, messageSourceFactory)
    val moduleManager = initModules(config, jobManager, messageSourceFactory, rt)
    val resultHandler = new TxResultHandler(config, jobManager, args)

    // initialize the console
    val console = new CLIConsole(rt, jobManager, messageSourceFactory, moduleManager, resultHandler)

    // if arguments were not passed, stop.
    args.filterShellArgs.toList match {
      case Nil => console.shell()
      case params =>
        args.scriptFile(params) match {
          case Some(scriptFile) => console.executeScript(scriptFile)
          case None => console.execute(params mkString " ")
        }
    }
  }

  private def loadConfiguration(nonInteractiveMode: Boolean) = {
    if(!nonInteractiveMode) {
      logger.info(s"Loading configuration file '${TxConfig.configFile}'...")
    }
    Try(TxConfig.load(TxConfig.configFile)) match {
      case Success(cfg) => cfg
      case Failure(_) =>
        val cfg = TxConfig.defaultConfig
        if (!TxConfig.configFile.exists()) {
          logger.info(s"Creating default configuration file (${TxConfig.configFile.getAbsolutePath})...")
          cfg.save(TxConfig.configFile)
        }
        cfg
    }
  }

  private def initKafkaSandbox(config: TxConfig) = {
    logger.info("Starting Kafka Sandbox...")
    val kafkaSandbox = KafkaSandbox()
    config.zooKeeperConnect = kafkaSandbox.getConnectString
    Thread.sleep(3.seconds.toMillis)
  }

  /**
    *
    * Create the module manager and loads the built-in modules
    * @param config               the given [[TxConfig configuration]]
    * @param jobManager           the given [[JobManager job manager]]
    * @param messageSourceFactory the given [[MessageSourceFactory message source factory]]
    * @param rt                   the given [[TxRuntimeContext runtime context]]
    * @return a new [[ModuleManager module manager]] instance
    */
  private def initModules(config: TxConfig,
                          jobManager: JobManager,
                          messageSourceFactory: MessageSourceFactory,
                          rt: TxRuntimeContext) = {
    val moduleManager = new ModuleManager()(rt)
    moduleManager ++= Seq(
      new CoreModule(config, jobManager, moduleManager),
      new KafkaModule(config),
      new ZookeeperModule(config))

    // set the "active" module
    moduleManager.findModuleByName("core") foreach moduleManager.setActiveModule

    // initialize the message source factory
    moduleManager.modules foreach { module =>
      // add the message readers
      module match {
        case reader: Module with MessageReader =>
          module.supportedPrefixes.foreach(prefix => messageSourceFactory.addReader(prefix, reader))
        case _ =>
      }

      // add the message writers
      module match {
        case writer: Module with MessageWriter =>
          module.supportedPrefixes.foreach(prefix => messageSourceFactory.addWriter(prefix, writer))
        case _ =>
      }
    }
    moduleManager
  }

}