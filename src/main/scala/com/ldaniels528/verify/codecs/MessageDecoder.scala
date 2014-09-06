package com.ldaniels528.verify.codecs

import scala.util.Try

/**
 * Message Decoder
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
trait MessageDecoder[T] {

  /**
   * Decodes the binary message into a typed object
   */
  def decode(message: Array[Byte]): Try[T]

}
