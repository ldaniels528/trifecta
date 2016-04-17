package com.github.ldaniels528.trifecta.io

import com.github.ldaniels528.trifecta.messages.MessageDecoder

import scala.concurrent.ExecutionContext

/**
 * This trait should be implemented by classes that are interested in serving as an
 * output device for writing binary messages
 * @author lawrence.daniels@gmail.com
 */
trait OutputSource {

  /**
    * Opens the output source for writing
    */
  def open(): Unit

  /**
   * Writes the given key and decoded message to the underlying stream
   * @param data the given key and message
   * @return the response value
   */
  def write(data: KeyAndMessage, decoder: Option[MessageDecoder[_]] = None)(implicit ec: ExecutionContext): Unit

  /**
   * Closes the underlying stream
   */
  def close(): Unit

}
