package com.github.ldaniels528.trifecta.messages.codec

import java.io.File

import com.github.ldaniels528.trifecta.TxConfig
import com.github.ldaniels528.trifecta.TxConfig.{TxFailedSchema, TxSuccessSchema}
import com.github.ldaniels528.trifecta.messages.codec.apache.ApacheAccessLogDecoder
import com.github.ldaniels528.trifecta.messages.codec.avro.AvroCodec
import com.github.ldaniels528.trifecta.messages.codec.gzip.GzipCodec
import com.github.ldaniels528.trifecta.messages.codec.json.{JsonHelper, JsonMessageDecoder}
import org.slf4j.LoggerFactory

import scala.collection.concurrent.TrieMap
import scala.io.Source
import scala.util.{Failure, Success, Try}

/**
  * Message CODEC Factory
  * @author lawrence.daniels@gmail.com
  */
object MessageCodecFactory {
  private lazy val logger = LoggerFactory.getLogger(getClass)
  private val decoders = TrieMap[String, MessageDecoder[_]](
    "apachelog" -> ApacheAccessLogDecoder,
    "bytes" -> LoopBackCodec,
    "gzip" -> GzipCodec,
    "json" -> JsonMessageDecoder,
    "text" -> PlainTextCodec
  )
  private val encoders = TrieMap[String, MessageEncoder[_]](
    "bytes" -> LoopBackCodec,
    "gzip" -> GzipCodec,
    "text" -> PlainTextCodec
  )

  /**
    * Optionally returns a message decoder for the given URL
    * @param config the given [[TxConfig configuration]]
    * @param url    the given message decoder URL (e.g. "avro:file:avro/quotes.avsc")
    * @return an option of a [[MessageDecoder]]
    */
  def getDecoder(config: TxConfig, url: String): Option[MessageDecoder[_]] = {
    url match {
      case s if s.startsWith("avro:") => Option(AvroCodec.resolve(s.drop(5)))
      case s if s.startsWith("decoder:") =>
        config.getDecoders.find(_.name == s.drop(8)) map (_.decoder match {
          case TxSuccessSchema(_, avroDecoder, _) => avroDecoder
          case TxFailedSchema(_, cause, _) => throw new IllegalStateException(cause.getMessage)
        })
      case name => decoders.get(name)
    }
  }

  /**
    * Optionally returns a message encoder for the given URL
    * @param config the given [[TxConfig configuration]]
    * @param url    the given message encoder URL (e.g. "bytes")
    * @return an option of a [[MessageEncoder]]
    */
  def getEncoder(config: TxConfig, url: String): Option[MessageEncoder[_]] = {
    encoders.find { case (name, _) => name == url } map { case (_, encoder) => encoder }
  }

  def loadUserDefinedCodecs(classLoader: ClassLoader, file: File) {
    if (file.exists()) {
      Try {
        logger.info("Loading user-defined message CODECs...")
        val jsonString = Source.fromFile(file).getLines() mkString "\n"
        JsonHelper.transform[List[UserDecoder]](jsonString)
      } match {
        case Success(codecDefs) =>
          var count = 0
          for {
            codecDef <- codecDefs
            codec <- Try(classLoader.loadClass(codecDef.`class`).newInstance()) match {
              case Success(potentialCodec) => List(potentialCodec)
              case Failure(e) =>
                logger.warn(s"Failed to load message CODEC '${codecDef.name}' (${codecDef.`class`}): ${e.getMessage}")
                Nil
            }
          } {
            count += (codec match {
              case de: MessageDecoder[_] with MessageEncoder[_] =>
                decoders.put(codecDef.name, de)
                encoders.put(codecDef.name, de)
                2
              case d: MessageDecoder[_] =>
                decoders.put(codecDef.name, d)
                1
              case e: MessageEncoder[_] =>
                encoders.put(codecDef.name, e)
                1
              case _ =>
                logger.warn(s"${codecDef.name} (${codecDef.`class`}) was neither a message decoder or encoder")
                0
            })
          }
          logger.info(s"Loaded $count user-defined message CODECs")
        case Failure(e) =>
          logger.warn(s"Failed to load user-defined message CODECs: ${e.getMessage}")
      }
    }
  }

  /**
    * Represents a user-defined decoder
    * @param name    the name of the decoder
    * @param `class` the class name of the decoder
    */
  case class UserDecoder(name: String, `class`: String)

  /**
    * Loop-back CODEC
    * @author lawrence.daniels@gmail.com
    */
  object LoopBackCodec extends MessageDecoder[Array[Byte]] with MessageEncoder[Array[Byte]] {

    /**
      * Decodes the binary message into a typed object
      * @param message the given binary message
      * @return a decoded message wrapped in a Try-monad
      */
    override def decode(message: Array[Byte]): Try[Array[Byte]] = Success(message)

    /**
      * Encodes the binary message into a typed object
      * @param message the given binary message
      * @return a encoded message wrapped in a Try-monad
      */
    override def encode(message: Array[Byte]): Try[Array[Byte]] = Success(message)
  }

  /**
    * Plain-Text CODEC
    * @author lawrence.daniels@gmail.com
    */
  object PlainTextCodec extends MessageDecoder[String] with MessageEncoder[String] {

    /**
      * Decodes the binary message into a typed object
      * @param message the given binary message
      * @return a decoded message wrapped in a Try-monad
      */
    override def decode(message: Array[Byte]): Try[String] = Success(new String(message))

    /**
      * Encodes the binary message into a typed object
      * @param message the given binary message
      * @return a encoded message wrapped in a Try-monad
      */
    override def encode(message: Array[Byte]): Try[String] = Success(new String(message))
  }

}
