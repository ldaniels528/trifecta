package com.github.ldaniels528.trifecta.messages.codec.apache

import com.github.ldaniels528.trifecta.messages.codec.MessageDecoder
import com.github.ldaniels528.trifecta.messages.codec.avro.JsonTransCoding
import com.github.ldaniels528.trifecta.messages.codec.json.JsonHelper
import com.github.ldaniels528.trifecta.messages.logic.JsonMessageEvaluation
import net.liftweb.json.JValue

import scala.util.Try

/**
  * Message Decoder for Apache Access Log files
  * @author lawrence.daniels@gmail.com
  */
object ApacheAccessLogDecoder extends MessageDecoder[ApacheAccessLog]
  with JsonTransCoding with JsonMessageEvaluation {

  /**
    * Decodes the binary message into a JSON value
    * @param message the given binary message
    * @return a [[JValue JSON value]] wrapped in a Try-monad
    */
  override def decode(message: Array[Byte]): Try[ApacheAccessLog] = Try {
    ApacheAccessLog(new String(message))
  }

  /**
    * Decodes the binary message into a JSON object
    * @param message the given binary message
    * @return a decoded message wrapped in a Try-monad
    */
  override def decodeAsJson(message: Array[Byte]): Try[JValue] = {
    decode(message) map JsonHelper.toJson
  }

}
