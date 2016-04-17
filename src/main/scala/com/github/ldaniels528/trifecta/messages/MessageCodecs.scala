package com.github.ldaniels528.trifecta.messages

import com.github.ldaniels528.trifecta.TxConfig
import com.github.ldaniels528.trifecta.io.avro.{AvroCodec, AvroDecoder}
import com.github.ldaniels528.trifecta.io.gzip.GzipCodec
import com.github.ldaniels528.trifecta.io.json.JsonDecoder

import scala.util.{Success, Try}

/**
  * Trifecta Message CODEC Factory
  * @author lawrence.daniels@gmail.com
  */
object MessageCodecs {

  /**
    * Optionally returns a message decoder for the given URL
    * @param url the given message decoder URL (e.g. "avro:file:avro/quotes.avsc")
    * @return an option of a [[MessageDecoder]]
    */
  def getDecoder(config: TxConfig, url: String): Option[MessageDecoder[_]] = {
    url match {
      case s if s.startsWith("avro:") => Option(AvroCodec.resolve(s.drop(5)))
      case "bytes" => Option(LoopBackCodec)
      case s if s.startsWith("decoder:") =>
        config.getDecoders.find(_.name == s.drop(8)) map (_.decoder match {
          case Left(avroDecoder) => avroDecoder
          case Right(cause) => throw new IllegalStateException(cause.error)
        })
      case "json" => Option(JsonDecoder)
      case "gzip" => Option(GzipCodec)
      case "text" => Option(PlainTextCodec)
      case _ => None
    }
  }

  /**
    * Optionally returns a message encoder for the given URL
    * @param url the given message encoder URL (e.g. "bytes")
    * @return an option of a [[MessageEncoder]]
    */
  def getEncoder(url: String): Option[MessageEncoder[_]] = {
    url match {
      case "bytes" => Option(LoopBackCodec)
      case "gzip" => Option(GzipCodec)
      case "text" => Option(PlainTextCodec)
      case _ => None
    }
  }

  /**
    * Returns the type name of the given message decoder
    * @param decoder the given message decoder
    * @return the type name (e.g. "json")
    */
  def getTypeName(decoder: MessageDecoder[_]): String = decoder match {
    case av: AvroDecoder => "avro"
    case JsonDecoder => "json"
    case GzipCodec => "gzip"
    case LoopBackCodec => "bytes"
    case PlainTextCodec => "text"
    case _ => "unknown"
  }

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
