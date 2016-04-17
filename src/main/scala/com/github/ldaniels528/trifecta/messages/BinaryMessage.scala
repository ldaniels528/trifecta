package com.github.ldaniels528.trifecta.messages

/**
 * Represents a binary message
 * @author lawrence.daniels@gmail.com
 */
trait BinaryMessage {

  /**
   * Returns the message's binary key
   * @return the message's binary key
   */
  def key: Array[Byte]

  /**
   * Returns the actual message data
   * @return the actual message data
   */
  def message: Array[Byte]

  /**
   * Returns the message's offset
   * @return the message's offset
   */
  def offset: Long

}
