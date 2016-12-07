package com.github.ldaniels528.trifecta

import com.github.ldaniels528.trifecta.io.kafka.KafkaSandbox
import com.github.ldaniels528.trifecta.messages.{MessageReader, MessageSourceFactory, MessageWriter}
import com.github.ldaniels528.trifecta.modules.azure.AzureModule
import com.github.ldaniels528.trifecta.modules.cassandra.CassandraModule
import com.github.ldaniels528.trifecta.modules.core.CoreModule
import com.github.ldaniels528.trifecta.modules.elasticsearch.ElasticSearchModule
import com.github.ldaniels528.trifecta.modules.etl.ETLModule
import com.github.ldaniels528.trifecta.modules.kafka.KafkaModule
import com.github.ldaniels528.trifecta.modules.mongodb.MongoModule
import com.github.ldaniels528.trifecta.modules.zookeeper.ZookeeperModule
import com.github.ldaniels528.trifecta.modules.{Module, ModuleManager}
import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContext.Implicits.global
import scala.language.postfixOps
import scala.util.{Failure, Success, Try}

/**
  * Trifecta Shell (REPL)
  * @author lawrence.daniels@gmail.com
  */
object TrifectaShell {
  private val logger = LoggerFactory.getLogger(getClass)
  val VERSION = CoreModule.VERSION

  /**
    * Application entry point
    * @param args the given command line arguments
    */
  def main(args: Array[String]) {
    // use the ANSI console plugin to display the title line
    println(s"Trifecta v$VERSION")

    // load the configuration
    val config = loadConfiguration()

    // startup the Kafka Sandbox?
    if (args.contains("--kafka-sandbox")) {
      initKafkaSandbox(config)
    }

    // create the dependencies
    val jobManager = new JobManager()
    val messageSourceFactory = new MessageSourceFactory()
    val rt = TxRuntimeContext(config, messageSourceFactory)
    val moduleManager = initModules(config, jobManager, messageSourceFactory, rt)

    // interactive mode?
    val nonInteractiveMode = args.exists(!_.startsWith("--"))
    val resultHandler = new TxResultHandler(config, jobManager, nonInteractiveMode)

    // initialize the console
    val console = new CLIConsole(rt, jobManager, messageSourceFactory, moduleManager, resultHandler)

    // if arguments were not passed, stop.
    args.filterNot(_.startsWith("--")).toList match {
      case Nil =>
        console.shell()
      case params =>
        val line = params mkString " "
        console.execute(line)
    }
  }

  private def loadConfiguration() = {
    logger.info(s"Loading configuration file '${TxConfig.configFile}'...")
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
    Thread.sleep(3000)
  }

  /**
    *
    * Create the module manager and loads the built-in modules
    * @param config the given [[TxConfig configuration]]
    * @param jobManager the given [[JobManager job manager]]
    * @param messageSourceFactory the given [[MessageSourceFactory message source factory]]
    * @param rt the given [[TxRuntimeContext runtime context]]
    * @return a new [[ModuleManager module manager]] instance
    */
  private def initModules(config: TxConfig,
                          jobManager: JobManager,
                          messageSourceFactory: MessageSourceFactory,
                          rt: TxRuntimeContext) = {
    val moduleManager = new ModuleManager()(rt)
    moduleManager ++= Seq(
      new CoreModule(config, jobManager, moduleManager),
      new AzureModule(config),
      new CassandraModule(config),
      new ElasticSearchModule(config),
      new ETLModule(config),
      new KafkaModule(config),
      new MongoModule(config),
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