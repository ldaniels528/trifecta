package com.ldaniels528.trifecta.modules.core

import java.io.FileOutputStream

import com.ldaniels528.trifecta.modules.io.OutputWriter
import com.ldaniels528.trifecta.support.kafka.KafkaMicroConsumer.MessageData

import scala.concurrent.ExecutionContext

/**
 * File Output Writer
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class FileOutputWriter(path: String) extends OutputWriter {
  private val out = new FileOutputStream(path)

  override def write(message: MessageData)(implicit ec: ExecutionContext) = out.write(message.message)

  override def close() = out.close()

}
