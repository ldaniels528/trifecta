package com.github.ldaniels528.trifecta.io.json

import com.github.ldaniels528.trifecta.messages.MessageDecoder
import net.liftweb.json._

import scala.util.Try

/**
 * JSON Message Decoder
 * @author lawrence.daniels@gmail.com
 */
object JsonDecoder extends MessageDecoder[JValue] with JsonTranscoding {

  /**
   * Decodes the binary message into a typed object
   * @param message the given binary message
   * @return a decoded message wrapped in a Try-monad
   */
  override def decode(message: Array[Byte]): Try[JValue] = Try(parse(new String(message)))

  /**
   * Transcodes the given bytes into JSON
   * @param bytes the given byte array
   * @return a JSON value
   */
  override def toJSON(bytes: Array[Byte]): Try[JValue] = decode(bytes)

}
