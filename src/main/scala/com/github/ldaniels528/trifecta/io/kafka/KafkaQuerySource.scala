package com.github.ldaniels528.trifecta.io.kafka

import com.github.ldaniels528.trifecta.io.AsyncIO.IOCounter
import com.github.ldaniels528.trifecta.io.avro.AvroMessageDecoding
import com.github.ldaniels528.trifecta.io.kafka.KafkaQuerySource._
import com.github.ldaniels528.trifecta.io.zookeeper.ZKProxy
import com.github.ldaniels528.trifecta.messages.logic.Condition
import com.github.ldaniels528.trifecta.messages.query.{KQLResult, KQLSource}
import com.github.ldaniels528.trifecta.messages.{BinaryMessage, MessageDecoder}
import org.apache.avro.generic.GenericRecord
import org.apache.avro.util.Utf8
import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success}

/**
 * Kafka Query Source
 * @author lawrence.daniels@gmail.com
 */
case class KafkaQuerySource(topic: String, brokers: Seq[Broker], correlationId: Int = 0)(implicit zk: ZKProxy)
  extends KQLSource {
  private lazy val logger = LoggerFactory.getLogger(getClass)

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
      KQLResult(topic, myFields, values, elapsedTimeMillis)
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
    val avDecoder: AvroMessageDecoding = decoder match {
      case av: AvroMessageDecoding => av
      case dec =>
        logger.error(s"Wanted ${classOf[AvroMessageDecoding].getName} but found ${dec.getClass.getName}")
        throw new IllegalStateException("Only Avro decoding is supported")
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