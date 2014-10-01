package com.ldaniels528.trifecta.support.io

import scala.concurrent.ExecutionContext

/**
 * This trait should be implemented by classes that are interested in serving as an
 * output source for writing binary messages
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
trait BinaryOutputHandler extends OutputHandler {

  /**
   * Writes the given key-message pair to the underlying stream
   * @param key the given key
   * @param message the given message
   * @return the response value
   */
  def write(key: Array[Byte], message: Array[Byte])(implicit ec: ExecutionContext): Any

}
