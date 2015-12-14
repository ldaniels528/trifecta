package com.github.ldaniels528.trifecta.io.elasticsearch

import com.github.ldaniels528.trifecta.io.avro.AvroDecoder
import com.github.ldaniels528.trifecta.io.{KeyAndMessage, OutputSource}
import com.github.ldaniels528.trifecta.messages.MessageDecoder
import org.apache.avro.generic.GenericRecord

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext}
import scala.util.{Failure, Success}

/**
 * Elastic Search Document Output Source
 * @author lawrence.daniels@gmail.com
 */
class DocumentOutputSource(client: TxElasticSearchClient, index: String, objType: String, id: String) extends OutputSource {

  override def open() = ()

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

  /**
   * Replaces the given identifier with with the value found in the given mapping
   * @param id the given identifier (e.g. "$symbol")
   * @param record the given value mapping
   * @return the mapped value (e.g. "AAPL")
   */
  private def escape(id: String, record: GenericRecord): String = {
    if (!id.startsWith("$")) id
    else {
      val key = id.substring(1) // TODO handle recursively? - $$id
      Option(record.get(key)) map (String.valueOf(_)) map (_.replace(' ', '_')) getOrElse key
    }
  }

}
