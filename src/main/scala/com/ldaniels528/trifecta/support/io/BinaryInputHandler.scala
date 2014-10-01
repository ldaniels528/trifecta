package com.ldaniels528.trifecta.support.io

import scala.concurrent.ExecutionContext

/**
 * This trait should be implemented by classes that are interested in serving as an
 * input source for reading binary messages
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
trait BinaryInputHandler extends InputHandler {

  /**
   * Reads the given keyed-message from the underlying stream
   * @return a [[KeyedMessage]]
   */
  def read(implicit ec: ExecutionContext): KeyedMessage

}
