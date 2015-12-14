package com.github.ldaniels528.trifecta.messages

import scala.util.Try

/**
 * Message Encoder
 * @author lawrence.daniels@gmail.com
 */
trait MessageEncoder[T] {

  /**
   * Encodes the binary message into a typed object
   * @param message the given binary message
   * @return a encoded message wrapped in a Try-monad
   */
  def encode(message: Array[Byte]): Try[T]

  /**
   * Returns the string representation of the message decoder
   * @return the string representation of the message decoder
   */
  override def toString = getClass.getSimpleName

}
