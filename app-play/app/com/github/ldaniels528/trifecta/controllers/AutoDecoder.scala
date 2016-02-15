package com.github.ldaniels528.trifecta.controllers

import com.github.ldaniels528.trifecta.io.json.JsonDecoder
import com.github.ldaniels528.trifecta.messages.MessageCodecs.{LoopBackCodec, PlainTextCodec}
import com.github.ldaniels528.trifecta.messages.MessageDecoder
import com.github.ldaniels528.commons.helpers.StringHelper._

import scala.util.Try

/**
  * Automatic Type-Sensing Message Decoder
  */
object AutoDecoder extends MessageDecoder[AnyRef] {

  /**
    * Decodes the binary message into a typed object
    * @param message the given binary message
    * @return a decoded message wrapped in a Try-monad
    */
  override def decode(message: Array[Byte]): Try[AnyRef] = {
    if (message.isPrintable) {
      val jsonDecoding = JsonDecoder.decode(message)
      if (jsonDecoding.isSuccess) jsonDecoding else PlainTextCodec.decode(message)
    }
    else LoopBackCodec.decode(message)
  }
}
