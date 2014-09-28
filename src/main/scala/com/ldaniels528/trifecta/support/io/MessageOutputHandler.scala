package com.ldaniels528.trifecta.support.io

import com.ldaniels528.trifecta.support.messaging.MessageDecoder

import scala.concurrent.ExecutionContext

/**
 * This trait should be implemented by classes that are interested in serving as an
 * output device for writing Avro or JSON messages
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
trait MessageOutputHandler extends OutputHandler {

  /**
   * Writes the given key and decoded message to the underlying stream
   * @param key the given key
   * @param message the given message
   * @return the response value
   */
  def write(decoder: Option[MessageDecoder[_]], key: Array[Byte], message: Array[Byte])(implicit ec: ExecutionContext): Any

}
