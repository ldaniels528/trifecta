package com.github.ldaniels528.trifecta.io.mongodb

import com.github.ldaniels528.trifecta.io.avro.AvroDecoder
import com.github.ldaniels528.trifecta.io.json.JsonHelper
import com.github.ldaniels528.trifecta.io.{KeyAndMessage, OutputSource}
import com.github.ldaniels528.trifecta.messages.MessageDecoder

import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success}

/**
 * MongoDB Output Source
 * @author lawrence.daniels@gmail.com
 */
class MongoOutputSource(mc: TxMongoCollection) extends OutputSource {

  override def open() = ()
  
  override def write(data: KeyAndMessage, decoder: Option[MessageDecoder[_]])(implicit ec: ExecutionContext) {
    decoder match {
      case Some(av: AvroDecoder) =>
        av.decode(data.message) match {
          case Success(record) =>
            mc.insert(JsonHelper.toJson(record.toString))
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
