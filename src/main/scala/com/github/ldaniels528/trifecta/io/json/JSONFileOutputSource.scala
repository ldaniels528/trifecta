package com.github.ldaniels528.trifecta.io.json

import java.io._

import com.github.ldaniels528.trifecta.io.{KeyAndMessage, OutputSource}
import com.github.ldaniels528.trifecta.messages.MessageDecoder
import com.github.ldaniels528.commons.helpers.OptionHelper.Risky._

import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success}

/**
  * JSON File Output Source
  * @author lawrence.daniels@gmail.com
  */
class JSONFileOutputSource(out: OutputStream) extends OutputSource {
  private var writer_? : Option[BufferedWriter] = None

  override def open() = writer_? = new BufferedWriter(new OutputStreamWriter(out))

  override def write(data: KeyAndMessage, decoder: Option[MessageDecoder[_]])(implicit ec: ExecutionContext) {
    for {
      dec <- decoder
      out <- writer_?
      record = dec.decode(data.message) match {
        case Success(v) => v
        case Failure(e) => throw new IllegalStateException("Message could not be decoded", e)
      }
    } {
      out.write(record.toString)
      out.newLine()
    }
  }

  override def close() = writer_? foreach { out =>
    out.flush()
    out.close()
  }

}

/**
  * File Output Handler Singleton
  * @author lawrence.daniels@gmail.com
  */
object JSONFileOutputSource {

  def apply(path: String) = new JSONFileOutputSource(new FileOutputStream(path))

  def apply(file: File) = new JSONFileOutputSource(new FileOutputStream(file))

}
