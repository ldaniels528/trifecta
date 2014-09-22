package com.ldaniels528.trifecta.support.messaging

import scala.util.Try

/**
 * Message Decoder
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
trait MessageDecoder[T] {

  /**
   * Decodes the binary message into a typed object
   * @param message the given binary message
   * @return a decoded message wrapped in a Try-monad
   */
  def decode(message: Array[Byte]): Try[T]

  /**
   * Returns the string representation of the message decoder
   * @return the string representation of the message decoder
   */
  override def toString = getClass.getSimpleName

}
