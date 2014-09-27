package com.ldaniels528.trifecta.modules.io

import scala.concurrent.ExecutionContext

/**
 * This trait should be implemented by classes that are interested in serving as an
 * output device for writing messages
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
trait OutputWriter {

  /**
   * Writes the given key-message pair to the underlying stream
   * @param key the given key
   * @param message the given message
   * @return any value
   */
  def write(key: Array[Byte], message: Array[Byte])(implicit ec: ExecutionContext): Any

  /**
   * Closes the underlying stream
   */
  def close(): Unit

}
