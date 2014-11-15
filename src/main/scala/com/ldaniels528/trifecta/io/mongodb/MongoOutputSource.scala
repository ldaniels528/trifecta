package com.ldaniels528.trifecta.io.mongodb

import com.ldaniels528.trifecta.io.avro.AvroDecoder
import com.ldaniels528.trifecta.io.json.TxJsonUtil
import com.ldaniels528.trifecta.io.{KeyAndMessage, OutputSource}
import com.ldaniels528.trifecta.messages.MessageDecoder

import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success}

/**
 * MongoDB Output Source
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class MongoOutputSource(mc: TxMongoCollection) extends OutputSource {

  /**
   * Writes the given key-message pair to the underlying stream
   * @param data the given key and message
   * @return the response value
   */
  override def write(data: KeyAndMessage, decoder: Option[MessageDecoder[_]])(implicit ec: ExecutionContext) {
    decoder match {
      case Some(av: AvroDecoder) =>
        av.decode(data.message) match {
          case Success(record) =>
            mc.insert(TxJsonUtil.toJson(record.toString))
            ()
          case Failure(e) =>
            throw new IllegalStateException(e.getMessage, e)
        }
      case Some(unhandled) =>
        throw new IllegalStateException(s"Unhandled decoder '$unhandled'")
      case None =>
        throw new IllegalStateException(s"No message decoder specified")
    }
  }

  override def close() = ()

}
