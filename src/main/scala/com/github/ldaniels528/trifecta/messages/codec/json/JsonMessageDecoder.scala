package com.github.ldaniels528.trifecta.messages.codec.json

import com.github.ldaniels528.trifecta.messages.codec.MessageDecoder
import net.liftweb.json.{JValue, _}

import scala.util.Try

/**
  * JSON Message Decoder
  * @author lawrence.daniels@gmail.com
  */
object JsonMessageDecoder extends MessageDecoder[JValue]
  with JsonTransCoding with JsonMessageEvaluation {

  /**
    * Decodes the binary message into a typed object
    * @param message the given binary message
    * @return a decoded message wrapped in a Try-monad
    */
  override def decode(message: Array[Byte]): Try[JValue] = Try(parse(new String(message)))

  /**
    * Decodes the binary message into a JSON object
    * @param message the given binary message
    * @return a decoded message wrapped in a Try-monad
    */
  override def decodeAsJson(message: Array[Byte]): Try[JValue] = decode(message)

}
