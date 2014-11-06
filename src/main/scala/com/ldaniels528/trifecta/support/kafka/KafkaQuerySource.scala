package com.ldaniels528.trifecta.support.kafka

import com.ldaniels528.trifecta.decoders.AvroDecoder
import com.ldaniels528.trifecta.support.io.query.{QueryResult, QuerySource}
import com.ldaniels528.trifecta.support.messaging.logic.Condition
import com.ldaniels528.trifecta.support.messaging.{BinaryMessage, MessageDecoder}
import com.ldaniels528.trifecta.support.zookeeper.ZKProxy
import org.apache.avro.generic.GenericRecord

import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success}

/**
 * Kafka Query Source
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
case class KafkaQuerySource(topic: String, brokers: Seq[Broker], correlationId: Int = 0)(implicit zk: ZKProxy)
  extends QuerySource {

  override def findAll(fields: Seq[String], decoder: MessageDecoder[_], conditions: Seq[Condition], limit: Option[Int])(implicit ec: ExecutionContext) = {
     KafkaMicroConsumer.findAll(topic, brokers, correlationId, conditions, limit) map {
      _ map (message => decodeMessage(message, decoder)) map { record =>
        Map(fields map (field => (field, record.get(field))): _*)
      }
    } map (QueryResult(fields, _))
  }

  /**
   * Decodes the given message
   * @param msg the given [[BinaryMessage]]
   * @param decoder the given message decoder
   * @return the decoded message
   */
  private def decodeMessage(msg: BinaryMessage, decoder: MessageDecoder[_]): GenericRecord = {
    // only Avro decoders are supported
    val avDecoder: AvroDecoder = decoder match {
      case av: AvroDecoder => av
      case _ => throw new IllegalStateException("Only Avro decoding is supported")
    }

    // decode the message
    avDecoder.decode(msg.message) match {
      case Success(record) => record
      case Failure(e) =>
        throw new IllegalStateException(e.getMessage, e)
    }
  }

}
