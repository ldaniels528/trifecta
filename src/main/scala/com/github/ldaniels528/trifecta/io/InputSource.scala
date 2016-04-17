package com.github.ldaniels528.trifecta.io

import com.github.ldaniels528.trifecta.messages.query.KQLSource

/**
 * This trait should be implemented by classes that are interested in serving as an
 * input source for reading binary messages
 * @author lawrence.daniels@gmail.com
 */
trait InputSource {

  /**
   * Reads the given keyed-message from the underlying stream
   * @return the option of a key-and-message
   */
  def read: Option[KeyAndMessage]

  /**
   * Returns a source for querying via Kafka Query Language (KQL)
   * @return the option of a query source
   */
  def getQuerySource: Option[KQLSource] = None

  /**
   * Closes the underlying stream
   */
  def close(): Unit

}
