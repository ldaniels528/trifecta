package com.ldaniels528.trifecta.support.io

import com.ldaniels528.trifecta.support.messaging.MessageDecoder

import scala.concurrent.ExecutionContext

/**
 * This trait should be implemented by classes that are interested in serving as an
 * output device for writing binary messages
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
trait OutputSource {

  /**
   * Writes the given key and decoded message to the underlying stream
   * @param data the given key and message
   * @return the response value
   */
  def write(data: KeyAndMessage, decoder: Option[MessageDecoder[_]] = None)(implicit ec: ExecutionContext): Any

  /**
   * Closes the underlying stream
   */
  def close(): Unit

}
