package com.github.ldaniels528.trifecta

import com.github.ldaniels528.commons.helpers.OptionHelper._
import com.github.ldaniels528.trifecta.TxConfig.TxSuccessSchema
import com.github.ldaniels528.trifecta.messages.codec.{CompositeMessageDecoder, MessageCodecFactory, MessageDecoder}
import com.github.ldaniels528.trifecta.messages.{MessageInputSource, MessageOutputSource, MessageSourceFactory}
import org.slf4j.LoggerFactory

import scala.collection.concurrent.TrieMap
import scala.concurrent.ExecutionContext

/**
  * Trifecta Runtime Context
  * @author lawrence.daniels@gmail.com
  */
case class TxRuntimeContext(config: TxConfig, messageSourceFactory: MessageSourceFactory)(implicit ec: ExecutionContext) {
  private[trifecta] val logger = LoggerFactory.getLogger(getClass)
  private implicit val cfg = config

  // support registering decoders
  private val decoders = TrieMap[String, MessageDecoder[_]]()
  private var once = true

  // load the default decoders
  config.getDecoders foreach { txDecoder =>
    txDecoder.decoder match {
      case TxSuccessSchema(_, decoder, _) => decoders += txDecoder.topic -> decoder
      case _ =>
    }
  }

  /**
    * Attempts to resolve the given topic or decoder URL into an actual message decoder
    * @param topicOrUrl the given topic or decoder URL
    * @return an option of a [[MessageDecoder]]
    */
  def resolveDecoder(topicOrUrl: String)(implicit rt: TxRuntimeContext): Option[MessageDecoder[_]] = {
    if (once) {
      config.getDecoders.filter(_.decoder.isSuccess).groupBy(_.topic) foreach { case (topic, txDecoders) =>
        rt.registerDecoder(topic, new CompositeMessageDecoder(txDecoders))
      }
      once = !once
    }
    decoders.get(topicOrUrl) ?? MessageCodecFactory.getDecoder(config, topicOrUrl)
  }

  /**
    * Returns the input handler for the given output URL
    * @param url the given input URL (e.g. "es:/quotes/quote/GDF")
    * @return an option of an [[MessageInputSource]]
    */
  def getInputHandler(url: String): Option[MessageInputSource] = {
    messageSourceFactory.findInputSource(url)
  }

  /**
    * Returns the output handler for the given output URL
    * @param url the given output URL
    * @return an option of an [[MessageOutputSource]]
    * @example {{{ getOutputHandler("es:/quotes/$exchange/$symbol") }}}
    */
  def getOutputHandler(url: String): Option[MessageOutputSource] = {
    messageSourceFactory.findOutputSource(url)
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

  def getDeviceURLWithDefault(prefix: String, deviceURL: String): String = {
    if (deviceURL.contains(':')) deviceURL else s"$prefix:$deviceURL"
  }

}
