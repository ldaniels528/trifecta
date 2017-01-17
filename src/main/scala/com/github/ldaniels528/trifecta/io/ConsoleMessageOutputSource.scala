package com.github.ldaniels528.trifecta.io

import java.io.PrintStream

import com.github.ldaniels528.trifecta.messages.codec.MessageDecoder
import com.github.ldaniels528.trifecta.messages.codec.json.{JsonHelper, JsonTransCoding}
import com.github.ldaniels528.trifecta.messages.{KeyAndMessage, MessageOutputSource}

import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success}

/**
  * Console Message Output Source
  * @author lawrence.daniels@gmail.com
  */
class ConsoleMessageOutputSource(out: PrintStream = System.out, encoding: String = "utf8") extends MessageOutputSource {
  /**
    * Opens the output source for writing
    */
  override def open(): Unit = ()

  /**
    * Writes the given key and decoded message to the underlying stream
    * @param data the given key and message
    */
  override def write(data: KeyAndMessage, decoder: Option[MessageDecoder[_]])(implicit ec: ExecutionContext): Unit = {
    decoder match {
      case Some(js: JsonTransCoding) =>
        js.decodeAsJson(data.message) match {
          case Success(result) =>
            out.println(JsonHelper.renderJson(result, pretty = true))
          case Failure(e) =>
            throw new IllegalStateException(e.getMessage, e)
        }
      case Some(unhandled) =>
        throw new IllegalStateException(s"Unhandled decoder '$unhandled'")
      case None => ()
    }
  }

  /**
    * Closes the underlying stream
    */
  override def close(): Unit = ()

}
