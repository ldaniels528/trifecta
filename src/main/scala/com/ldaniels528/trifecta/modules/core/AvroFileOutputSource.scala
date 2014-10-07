package com.ldaniels528.trifecta.modules.core

import java.io._

import com.ldaniels528.trifecta.support.io.{KeyAndMessage, OutputSource}
import com.ldaniels528.trifecta.support.messaging.MessageDecoder

import scala.concurrent.{Future, ExecutionContext}

/**
 * Avro File Output Source
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class AvroFileOutputSource(out: OutputStream) extends OutputSource {
  private val dos = new DataOutputStream(out)

  override def write(data: KeyAndMessage, decoder: Option[MessageDecoder[_]])(implicit ec: ExecutionContext) = Future {
    // persist the key
    dos.writeInt(data.key.length)
    dos.write(data.key)

    // persist the message
    dos.writeInt(data.message.length)
    dos.write(data.message)
  }

  override def close() = dos.close()

}

/**
 * File Output Handler Singleton
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
object AvroFileOutputSource {

  def apply(path: String): AvroFileOutputSource = {
    new AvroFileOutputSource(new FileOutputStream(path))
  }

  def apply(file: File): AvroFileOutputSource = {
    new AvroFileOutputSource(new FileOutputStream(file))
  }

}
