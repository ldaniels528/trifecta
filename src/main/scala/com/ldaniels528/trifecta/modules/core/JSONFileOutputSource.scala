package com.ldaniels528.trifecta.modules.core

import java.io._

import com.ldaniels528.trifecta.support.avro.AvroDecoder
import com.ldaniels528.trifecta.support.io.{KeyAndMessage, OutputSource}
import com.ldaniels528.trifecta.support.messaging.MessageDecoder

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

/**
 * JSON File Output Source
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class JSONFileOutputSource(out: OutputStream) extends OutputSource {
  private val writer = new BufferedWriter(new OutputStreamWriter(out))

  override def write(data: KeyAndMessage, decoder: Option[MessageDecoder[_]])(implicit ec: ExecutionContext) = Future {
    decoder match {
      case Some(av: AvroDecoder) =>
        av.decode(data.message) match {
          case Success(record) =>
            writer.write(record.toString)
            writer.newLine()
          case Failure(e) =>
            throw new IllegalStateException(e.getMessage, e)
        }
      case Some(unhandled) =>
        throw new IllegalStateException(s"Unhandled decoder '$unhandled'")
      case None =>
        throw new IllegalStateException(s"No message decoder specified")
    }
  }

  override def close() = writer.close()

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
