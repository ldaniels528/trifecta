package com.ldaniels528.trifecta.modules.core

import java.io._

import com.ldaniels528.trifecta.support.io.{KeyAndMessage, OutputSource}
import com.ldaniels528.trifecta.support.messaging.MessageDecoder

import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success}

/**
 * JSON File Output Source
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class JSONFileOutputSource(out: OutputStream) extends OutputSource {
  private val writer = new BufferedWriter(new OutputStreamWriter(out))

  /**
   * Writes the given key and decoded message to the underlying stream
   * @param data the given key and message
   * @return the response value
   */
  override def write(data: KeyAndMessage, decoder: Option[MessageDecoder[_]])(implicit ec: ExecutionContext) {
    for {
      dec <- decoder
      record = dec.decode(data.message) match {
        case Success(v) => v
        case Failure(e) => throw new IllegalStateException("Message could not be decoded", e)
      }
    } {
      writer.write(record.toString)
      writer.newLine()
    }
  }

  /**
   * Closes the underlying stream
   */
  override def close(): Unit = {
    writer.flush()
    writer.close()
  }

}

/**
 * File Output Handler Singleton
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
object JSONFileOutputSource {

  def apply(path: String): JSONFileOutputSource = {
    new JSONFileOutputSource(new FileOutputStream(path))
  }

  def apply(file: File): JSONFileOutputSource = {
    new JSONFileOutputSource(new FileOutputStream(file))
  }

}
