package com.ldaniels528.trifecta.io.kafka

import com.ldaniels528.trifecta.io.AsyncIO.IOCounter
import com.ldaniels528.trifecta.io.kafka.KafkaQuerySource._
import com.ldaniels528.trifecta.io.zookeeper.ZKProxy
import com.ldaniels528.trifecta.messages.logic.Condition
import com.ldaniels528.trifecta.messages.query.{QueryResult, QuerySource}
import com.ldaniels528.trifecta.messages.{BinaryMessage, MessageDecoder}
import org.apache.avro.generic.GenericRecord
import org.apache.avro.util.Utf8

import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success}

/**
 * Kafka Query Source
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
case class KafkaQuerySource(topic: String, brokers: Seq[Broker], correlationId: Int = 0)(implicit zk: ZKProxy)
  extends QuerySource {

  override def findAll(fields: Seq[String],
                       decoder: MessageDecoder[_],
                       conditions: Seq[Condition],
                       limit: Option[Int],
                       counter: IOCounter)(implicit ec: ExecutionContext) = {
    val myFields = fields.toList ::: List(Partition, Offset)
    val startTime = System.nanoTime()
    KafkaMicroConsumer.findAll(topic, brokers, correlationId, conditions, limit, counter) map {
      _ map { md =>
        counter.updateWriteCount(1)
        val record = decodeMessage(md, decoder)
        Map(fields map (field => (field, unwrapValue(record.get(field)))): _*) ++ Map(Partition -> md.partition, Offset -> md.offset)
      }
    } map { values =>
      val elapsedTimeMillis = (System.nanoTime() - startTime).toDouble / 1e9
      QueryResult(topic, myFields, values, elapsedTimeMillis)
    }
  }

  /**
   * Decodes the given message
   * @param msg the given [[BinaryMessage]]
   * @param decoder the given message decoder
   * @return the decoded message
   */
  private def decodeMessage(msg: BinaryMessage, decoder: MessageDecoder[_]): GenericRecord = {
    // only Avro decoders are supported
    val avDecoder: MessageDecoder[GenericRecord] = decoder match {
      case av: MessageDecoder[GenericRecord] => av
      case _ => throw new IllegalStateException("Only Avro decoding is supported")
    }

    // decode the message
    avDecoder.decode(msg.message) match {
      case Success(record) => record
      case Failure(e) =>
        throw new IllegalStateException(e.getMessage, e)
    }
  }

  private def unwrapValue(value: AnyRef): AnyRef = {
    value match {
      case u: Utf8 => u.toString
      case x => x
    }
  }

}

object KafkaQuerySource {
  val Partition = "__partition"
  val Offset = "__offset"

}