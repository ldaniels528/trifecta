package com.github.ldaniels528.trifecta.ui.controllers

import com.github.ldaniels528.trifecta.messages.codec.json.JsonMessageDecoder
import com.github.ldaniels528.trifecta.messages.codec.MessageCodecFactory.{LoopBackCodec, PlainTextCodec}
import com.github.ldaniels528.commons.helpers.StringHelper._
import com.github.ldaniels528.trifecta.messages.codec.MessageDecoder

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
      val jsonDecoding = JsonMessageDecoder.decode(message)
      if (jsonDecoding.isSuccess) jsonDecoding else PlainTextCodec.decode(message)
    }
    else LoopBackCodec.decode(message)
  }
}
