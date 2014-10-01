package com.ldaniels528.trifecta.support.io

/**
 * This trait should be implemented by classes that are interested in serving as an
 * input source for reading binary messages
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
trait InputHandler {

  /**
   * Reads the given keyed-message from the underlying stream
   * @return the option of a key-and-message
   */
  def read: Option[KeyAndMessage]

  /**
   * Closes the underlying stream
   */
  def close(): Unit

}
