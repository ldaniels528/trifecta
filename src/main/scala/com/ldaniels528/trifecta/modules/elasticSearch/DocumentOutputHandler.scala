package com.ldaniels528.trifecta.modules.elasticSearch

import com.ldaniels528.trifecta.support.avro.AvroDecoder
import com.ldaniels528.trifecta.support.elasticsearch.ElasticSearchDAO
import com.ldaniels528.trifecta.support.io.{KeyAndMessage, OutputHandler}
import com.ldaniels528.trifecta.support.messaging.MessageDecoder
import com.ldaniels528.trifecta.util.TxUtils._

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

/**
 * Elastic Search Document Output Handler
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class DocumentOutputHandler(client: ElasticSearchDAO, index: String, indexType: String, id: Option[String])
  extends OutputHandler {

  /**
   * Returns the binary encoding
   * @return the binary encoding
   */
  val encoding: String = "UTF8"

  /**
   * Writes the given key and decoded message to the underlying stream
   * @param data the given key and message
   * @return the response value
   */
  override def write(data: KeyAndMessage, decoder: Option[MessageDecoder[_]])(implicit ec: ExecutionContext): Future[_] = {
    decoder match {
      case Some(av: AvroDecoder) =>
        av.decode(data.message) match {
          case Success(record) =>
            val myId = id ?? (Option(data.key) map (new String(_, encoding)))
            client.createDocument(index, indexType, myId, record.toString, refresh = true)
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
