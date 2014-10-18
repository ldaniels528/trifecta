package com.ldaniels528.trifecta.modules.cassandra

import com.datastax.driver.core.ConsistencyLevel
import com.ldaniels528.trifecta.decoders.AvroDecoder
import com.ldaniels528.trifecta.support.cassandra.Casserole
import com.ldaniels528.trifecta.support.io.{KeyAndMessage, OutputSource}
import com.ldaniels528.trifecta.support.messaging.MessageDecoder

import scala.collection.JavaConversions._
import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success}

/**
 * Cassandra Output Source
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class CassandraOutputSource(conn: Casserole, keySpace: String, columnFamily: String, cl: ConsistencyLevel) extends OutputSource {
  private val session = conn.getSession(keySpace)

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
            val keyValues = (record.getSchema.getFields map (_.name) map (key => (key, record.get(key)))).toSeq
            session.insert(columnFamily, keyValues: _*)(cl)
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

  /**
   * Closes the underlying stream
   */
  override def close(): Unit = session.close()

}
