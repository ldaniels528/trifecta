package com.github.ldaniels528.trifecta.modules.cassandra

import com.github.ldaniels528.commons.helpers.OptionHelper.Risky._
import com.datastax.driver.core.ConsistencyLevel
import com.github.ldaniels528.trifecta.io.avro.AvroDecoder
import com.github.ldaniels528.trifecta.io.{KeyAndMessage, MessageOutputSource}
import com.github.ldaniels528.trifecta.messages.MessageDecoder

import scala.collection.JavaConversions._
import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success}

/**
 * Cassandra Output Source
 * @author lawrence.daniels@gmail.com
 */
class CassandraMessageOutputSource(conn: Casserole, keySpace: String, columnFamily: String, cl: ConsistencyLevel) extends MessageOutputSource {
  private var session_? : Option[CasseroleSession] = None

  override def open() = session_? = conn.getSession(keySpace)

  override def write(data: KeyAndMessage, decoder: Option[MessageDecoder[_]])(implicit ec: ExecutionContext) {
    decoder match {
      case Some(av: AvroDecoder) =>
        av.decode(data.message) match {
          case Success(record) =>
            val keyValues = (record.getSchema.getFields map (_.name) map (key => (key, record.get(key)))).toSeq
            session_?.foreach(_.insert(columnFamily, keyValues: _*)(cl))
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

  override def close(): Unit = session_?.foreach(_.close())

}
