package com.github.ldaniels528.trifecta.messages.codec.json

import net.liftweb.json.JValue

import scala.util.Try

/**
  * JSON Transcoding Capability
  * @author lawrence.daniels@gmail.com
  */
trait JsonTransCoding {

  /**
    * Decodes the binary message into a JSON object
    * @param message the given binary message
    * @return a decoded message wrapped in a Try-monad
    */
  def decodeAsJson(message: Array[Byte]): Try[JValue]

}
