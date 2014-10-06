package com.ldaniels528.trifecta.support.io

import com.ldaniels528.trifecta.support.messaging.MessageDecoder
import org.apache.avro.generic.GenericRecord

import scala.concurrent.{Future, ExecutionContext}

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
  def write(data: KeyAndMessage, decoder: Option[MessageDecoder[_]] = None)(implicit ec: ExecutionContext): Future[_]

  /**
   * Closes the underlying stream
   */
  def close(): Unit

  /**
   * Replaces the given identifier with with the value found in the given mapping
   * @param id the given identifier (e.g. "$symbol")
   * @param record the given value mapping
   * @return the mapped value (e.g. "AAPL")
   */
  protected def escape(id: String, record: GenericRecord): String = {
    if (!id.startsWith("$")) id
    else {
      val key = id.substring(1) // TODO handle recursively? - $$id
      Option(record.get(key)) map (String.valueOf(_)) getOrElse key
    }
  }

}
