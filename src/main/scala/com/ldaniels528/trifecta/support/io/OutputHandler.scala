package com.ldaniels528.trifecta.support.io

/**
 * This trait should be implemented by classes that are interested in serving as an
 * output device for writing binary messages
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
trait OutputHandler {

  /**
   * Closes the underlying stream
   */
  def close(): Unit

}
