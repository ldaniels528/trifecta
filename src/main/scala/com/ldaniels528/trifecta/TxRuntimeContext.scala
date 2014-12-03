package com.ldaniels528.trifecta

import com.ldaniels528.trifecta.command.parser.CommandParser
import com.ldaniels528.trifecta.command.parser.bdql.BigDataQueryParser
import com.ldaniels528.trifecta.io.AsyncIO.IOCounter
import com.ldaniels528.trifecta.io.{AsyncIO, InputSource, OutputSource}
import com.ldaniels528.trifecta.messages.logic.ConditionCompiler._
import com.ldaniels528.trifecta.messages.query.{BigDataQuery, BigDataSelection}
import com.ldaniels528.trifecta.messages.{MessageCodecs, MessageDecoder}
import com.ldaniels528.trifecta.modules._
import com.ldaniels528.trifecta.util.OptionHelper._
import com.ldaniels528.trifecta.util.StringHelper._
import org.slf4j.LoggerFactory

import scala.collection.concurrent.TrieMap
import scala.concurrent.ExecutionContext
import scala.util.Try

/**
 * Trifecta Runtime Context
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
case class TxRuntimeContext(config: TxConfig)(implicit ec: ExecutionContext) {
  private[trifecta] val logger = LoggerFactory.getLogger(getClass)
  private implicit val cfg = config

  // create the result handler
  private val resultHandler = new TxResultHandler(config)

  // support registering decoders
  private val decoders = TrieMap[String, MessageDecoder[_]]()

  // create the module manager and load the built-in modules
  val moduleManager = new ModuleManager()(this)
  moduleManager ++= Seq(
    new CassandraModule(config),
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
    val (prefix, _) = parseSourceURL(url).orDie(s"Malformed input source URL: $url")

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
    val (prefix, _) = parseSourceURL(url).orDie(s"Malformed output source URL: $url")

    // locate the module
    moduleManager.findModuleByPrefix(prefix) flatMap (_.getOutputSource(url))
  }

  def handleResult(result: Any, input: String)(implicit ec: ExecutionContext) = {
    resultHandler.handleResult(result, input)
  }

  def interpret(input: String): Try[Any] = {
    input match {
      case s if s.startsWith("`") && s.endsWith("`") => executeCommand(s.drop(1).dropRight(1))
      case s => interpretCommandLine(s)
    }
  }

  /**
   * Attempts to retrieve a message decoder by name
   * @param name the name of the desired [[MessageDecoder]]
   * @return an option of a [[MessageDecoder]]
   */
  def lookupDecoderByName(name: String): Option[MessageDecoder[_]] = decoders.get(name)

  /**
   * Registers a message decoder, which can be later retrieved by name
   * @param name the name of the [[MessageDecoder]]
   * @param decoder the [[MessageDecoder]] instance
   */
  def registerDecoder(name: String, decoder: MessageDecoder[_]): Unit = decoders(name) = decoder

  def shutdown(): Unit = moduleManager.shutdown()

  /**
   * Executes a local system command
   * @example `ps -ef`
   */
  private def executeCommand(command: String): Try[String] = {
    import scala.sys.process._

    Try(command.!!)
  }

  /**
   * Executes the given query
   * @param query the given [[BigDataQuery]]
   */
  def executeQuery(query: BigDataQuery)(implicit ec: ExecutionContext): AsyncIO = {
    val counter = IOCounter(startTimeMillis = System.currentTimeMillis())
    val task = query match {
      case BigDataSelection(source, destination, fields, criteria, limit) =>
        // get the input source and its decoder
        val inputSource: Option[InputSource] = getInputHandler(getDeviceURLWithDefault("topic", source.deviceURL))
        val inputDecoder: Option[MessageDecoder[_]] = lookupDecoderByName(source.decoderURL) ?? MessageCodecs.getDecoder(source.decoderURL)

        // get the output source and its encoder
        val outputSource: Option[OutputSource] = destination.flatMap(src => getOutputHandler(src.deviceURL))
        val outputDecoder: Option[MessageDecoder[_]] = destination.flatMap(src => MessageCodecs.getDecoder(src.decoderURL))

        // compile conditions & get all other properties
        val conditions = criteria.map(compile(_, inputDecoder)).toSeq
        val maximum = limit ?? Some(25)

        // perform the query/copy operation
        if (outputSource.nonEmpty) throw new IllegalStateException("Insert is not yet supported")
        else {
          val querySource = inputSource.flatMap(_.getQuerySource).orDie(s"No query compatible source found for URL '${source.deviceURL}'")
          val decoder = inputDecoder.orDie(s"No decoder found for URL ${source.decoderURL}")
          querySource.findAll(fields, decoder, conditions, maximum, counter)
        }
      case _ =>
        throw new IllegalStateException(s"Invalid query type - ${query.getClass.getName}")
    }
    AsyncIO(task, counter)
  }

  private def getDeviceURLWithDefault(prefix: String, deviceURL: String): String = {
    if (deviceURL.contains(':')) deviceURL else s"$prefix:$deviceURL"
  }

  /**
   * Interprets command line input
   * @param input the given line of input
   * @return a try-monad wrapped result
   */
  private def interpretCommandLine(input: String): Try[Any] = Try {
    // is the input a query?
    if (input.startsWith("select")) executeQuery(BigDataQueryParser(input))
    else {
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
        // verify and execute the command
        command.params.checkArgs(command, tokens)
        val result = command.fx(unixArgs)

        // auto-switch modules?
        if (config.autoSwitching && (command.promptAware || command.module.moduleName != "core")) {
          moduleManager.setActiveModule(command.module)
        }
        result
      }
    }
  }

  /**
   * Parses the the prefix and path from the I/O source URL
   * @param url the I/O source URL
   * @return the tuple represents the prefix and path
   */
  private def parseSourceURL(url: String): Option[(String, String)] = {
    url.indexOptionOf(":") map url.splitAt
  }

}
