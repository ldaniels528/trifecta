package com.ldaniels528.trifecta.decoders

import net.liftweb.json.JsonAST.JValue

import scala.util.Try

/**
 * This trait adds JSON transcoding support
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
trait JsonTranscoding {

  /**
   * Transcodes the given bytes into JSON
   * @param bytes the given byte array
   * @return a JSON value
   */
  def toJSON(bytes: Array[Byte]): Try[JValue]

}
