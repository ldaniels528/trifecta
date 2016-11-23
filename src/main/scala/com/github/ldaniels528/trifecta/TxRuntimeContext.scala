package com.github.ldaniels528.trifecta

import com.github.ldaniels528.commons.helpers.OptionHelper._
import com.github.ldaniels528.commons.helpers.StringHelper._
import com.github.ldaniels528.trifecta.io.{MessageInputSource, MessageOutputSource}
import com.github.ldaniels528.trifecta.messages.{CompositeTxDecoder, MessageCodecs, MessageDecoder}
import com.github.ldaniels528.trifecta.modules._
import com.github.ldaniels528.trifecta.modules.kafka.KafkaModule
import com.github.ldaniels528.trifecta.modules.zookeeper.ZookeeperModule
import org.slf4j.LoggerFactory

import scala.collection.concurrent.TrieMap
import scala.concurrent.ExecutionContext

/**
  * Trifecta Runtime Context
  * @author lawrence.daniels@gmail.com
  */
case class TxRuntimeContext(config: TxConfig)(implicit ec: ExecutionContext) {
  private[trifecta] val logger = LoggerFactory.getLogger(getClass)
  private implicit val cfg = config

  // support registering decoders
  private val decoders = TrieMap[String, MessageDecoder[_]]()
  private var once = true

  // load the default decoders
  config.getDecoders foreach { txDecoder =>
    txDecoder.decoder match {
      case Left(decoder) => decoders += txDecoder.topic -> decoder
      case _ =>
    }
  }

  // create the module manager and load the built-in modules
  val moduleManager = new ModuleManager()(this)
  moduleManager ++= Seq(
    new KafkaModule(config),
    new ZookeeperModule(config))

  // set the "active" module
  moduleManager.findModuleByName("core") foreach moduleManager.setActiveModule

  /**
    * Attempts to resolve the given topic or decoder URL into an actual message decoder
    * @param topicOrUrl the given topic or decoder URL
    * @return an option of a [[MessageDecoder]]
    */
  def resolveDecoder(topicOrUrl: String)(implicit rt: TxRuntimeContext): Option[MessageDecoder[_]] = {
    if (once) {
      config.getDecoders.filter(_.decoder.isLeft).groupBy(_.topic) foreach { case (topic, txDecoders) =>
        rt.registerDecoder(topic, new CompositeTxDecoder(txDecoders))
      }
      once = !once
    }
    decoders.get(topicOrUrl) ?? MessageCodecs.getDecoder(config, topicOrUrl)
  }

  /**
    * Returns the input handler for the given output URL
    * @param url the given input URL (e.g. "es:/quotes/quote/GDF")
    * @return an option of an [[MessageInputSource]]
    */
  def getInputHandler(url: String): Option[MessageInputSource] = {
    // get just the prefix
    val (prefix, _) = parseSourceURL(url).orDie(s"Malformed input source URL: $url")

    // locate the module
    moduleManager.findModuleByPrefix(prefix) flatMap (_.getInputSource(url))
  }

  /**
    * Returns the output handler for the given output URL
    * @param url the given output URL
    * @return an option of an [[MessageOutputSource]]
    * @example {{{ getOutputHandler("es:/quotes/$exchange/$symbol") }}}
    */
  def getOutputHandler(url: String): Option[MessageOutputSource] = {
    // get just the prefix
    val (prefix, _) = parseSourceURL(url).orDie(s"Malformed output source URL: $url")

    // locate the module
    moduleManager.findModuleByPrefix(prefix) flatMap (_.getOutputSource(url))
  }

  /**
    * Attempts to retrieve a message decoder by name
    * @param name the name of the desired [[MessageDecoder]]
    * @return an option of a [[MessageDecoder]]
    */
  def lookupDecoderByName(name: String): Option[MessageDecoder[_]] = decoders.get(name)

  /**
    * Registers a message decoder, which can be later retrieved by name
    * @param name    the name of the [[MessageDecoder]]
    * @param decoder the [[MessageDecoder]] instance
    */
  def registerDecoder(name: String, decoder: MessageDecoder[_]): Unit = decoders(name) = decoder

  /**
    * Releases all resources
    */
  def shutdown(): Unit = {
    logger.info("Shutting down...")
    moduleManager.shutdown()
  }

  def getDeviceURLWithDefault(prefix: String, deviceURL: String): String = {
    if (deviceURL.contains(':')) deviceURL else s"$prefix:$deviceURL"
  }

  /**
    * Parses the the prefix and path from the I/O source URL
    * @param url the I/O source URL
    * @return the tuple represents the prefix and path
    */
  private def parseSourceURL(url: String) = url.indexOptionOf(":") map url.splitAt

}
