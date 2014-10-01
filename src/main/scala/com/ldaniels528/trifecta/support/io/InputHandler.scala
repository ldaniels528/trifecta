package com.ldaniels528.trifecta.support.io

/**
 * This trait should be implemented by classes that are interested in serving as an
 * input source for reading binary messages
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
trait InputHandler {

  /**
   * Closes the underlying stream
   */
  def close(): Unit

}
