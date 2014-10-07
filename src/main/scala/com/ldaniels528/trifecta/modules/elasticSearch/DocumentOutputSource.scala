package com.ldaniels528.trifecta.modules.elasticSearch

import com.ldaniels528.trifecta.support.avro.AvroDecoder
import com.ldaniels528.trifecta.support.elasticsearch.TxElasticSearchClient
import com.ldaniels528.trifecta.support.io.{KeyAndMessage, OutputSource}
import com.ldaniels528.trifecta.support.messaging.MessageDecoder

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext}
import scala.util.{Failure, Success}

/**
 * Elastic Search Document Output Source
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class DocumentOutputSource(client: TxElasticSearchClient, index: String, objType: String, id: String)
  extends OutputSource {

  /**
   * Writes the given key and decoded message to the underlying stream
   * @param data the given key and message
   * @return the response value
   */
  override def write(data: KeyAndMessage, decoder: Option[MessageDecoder[_]])(implicit ec: ExecutionContext) {
    decoder match {
      case Some(av: AvroDecoder) =>
        av.decode(data.message) match {
          case Success(record) =>
            val task = client.create(
              index = escape(index, record),
              indexType = escape(objType, record),
              id = escape(id, record),
              doc = record.toString)
            Await.result(task, 60.seconds)
          case Failure(e) =>
            throw new IllegalStateException(e.getMessage, e)
        }
      case Some(unhandled) =>
        throw new IllegalStateException(s"Unhandled decoder '$unhandled'")
      case None =>
        throw new IllegalStateException(s"No message decoder specified")
    }
  }

  override def close(): Unit = ()

}
