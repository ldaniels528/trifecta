package com.kbb.trifecta.decoders

import com.github.ldaniels528.trifecta.messages.codec.MessageDecoder
import com.github.ldaniels528.trifecta.messages.codec.avro.JsonTransCoding
import com.github.ldaniels528.trifecta.messages.codec.json.JsonHelper
import com.github.ldaniels528.trifecta.messages.logic.JsonMessageEvaluation
import net.liftweb.json.JValue

import scala.util.Try

/**
  * KBB Log Raw Decoder
  * @author lawrence.daniels@gmail.com
  */
object LogRawMessageDecoder extends MessageDecoder[LogRawMessage]
  with JsonTransCoding with JsonMessageEvaluation {

  /**
    * Decodes the binary message into a JSON value
    * @param message the given binary message
    * @return a [[LogRawMessage JSON value]] wrapped in a Try-monad
    */
  override def decode(message: Array[Byte]): Try[LogRawMessage] = Try {
    LogRawMessage(message)
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
