package com.github.ldaniels528.trifecta.io.json

import net.liftweb.json.JsonAST.JValue

import scala.util.Try

/**
 * This trait adds JSON transcoding support
 * @author lawrence.daniels@gmail.com
 */
trait JsonTranscoding {

  /**
   * Transcodes the given bytes into JSON
   * @param bytes the given byte array
   * @return a JSON value
   */
  def toJSON(bytes: Array[Byte]): Try[JValue]

}
