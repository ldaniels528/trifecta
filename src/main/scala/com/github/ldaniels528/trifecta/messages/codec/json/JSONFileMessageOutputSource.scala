package com.github.ldaniels528.trifecta.messages.codec.json

import java.io._

import com.github.ldaniels528.commons.helpers.OptionHelper.Risky._
import com.github.ldaniels528.trifecta.messages.codec.MessageDecoder
import com.github.ldaniels528.trifecta.messages.{KeyAndMessage, MessageOutputSource}

import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success}

/**
  * JSON File Output Source
  * @author lawrence.daniels@gmail.com
  */
class JSONFileMessageOutputSource(out: OutputStream) extends MessageOutputSource {
  private var writer_? : Option[BufferedWriter] = None

  override def open(): Unit = writer_? = new BufferedWriter(new OutputStreamWriter(out))

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

  override def close(): Unit = writer_? foreach { out =>
    out.flush()
    out.close()
  }

}

/**
  * File Output Handler Singleton
  * @author lawrence.daniels@gmail.com
  */
object JSONFileMessageOutputSource {

  def apply(path: String) = new JSONFileMessageOutputSource(new FileOutputStream(path))

  def apply(file: File) = new JSONFileMessageOutputSource(new FileOutputStream(file))

}
