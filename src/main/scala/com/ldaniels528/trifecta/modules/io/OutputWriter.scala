package com.ldaniels528.trifecta.modules.io

import com.ldaniels528.trifecta.support.kafka.KafkaMicroConsumer.MessageData

import scala.concurrent.ExecutionContext

/**
 * This trait should be implemented by classes that are interested in serving as an
 * output device for writing messages
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
trait OutputWriter {

  def write(message: MessageData)(implicit ec: ExecutionContext): Any

  def close(): Unit

}
