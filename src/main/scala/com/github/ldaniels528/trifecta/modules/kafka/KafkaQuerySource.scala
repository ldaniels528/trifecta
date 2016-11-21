package com.github.ldaniels528.trifecta.modules.kafka

import com.github.ldaniels528.trifecta.io.AsyncIO.IOCounter
import com.github.ldaniels528.trifecta.io.avro.AvroMessageDecoding
import com.github.ldaniels528.trifecta.io.json.JsonDecoder
import com.github.ldaniels528.trifecta.messages.logic.Condition
import com.github.ldaniels528.trifecta.messages.query.{KQLResult, KQLSource}
import com.github.ldaniels528.trifecta.messages.{BinaryMessage, MessageDecoder}
import com.github.ldaniels528.trifecta.modules.kafka.KafkaQuerySource._
import com.github.ldaniels528.trifecta.modules.zookeeper.ZKProxy
import net.liftweb.json.JsonAST._
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
        val mapping = decodeMessage(md, decoder, fields)
        logger.info(s"mapping = $mapping")
        mapping ++ Map(Partition -> md.partition, Offset -> md.offset)
      }
    } map { values =>
      val elapsedTimeMillis = (System.nanoTime() - startTime).toDouble / 1e9
      KQLResult(topic, myFields, values, elapsedTimeMillis)
    }
  }

  /**
    * Decodes the given message
    * @param msg     the given [[BinaryMessage binary message]]
    * @param decoder the given message decoder
    * @return the decoded message
    */
  private def decodeMessage(msg: BinaryMessage, decoder: MessageDecoder[_], fields: Seq[String]) = {
    decoder match {
      case av: AvroMessageDecoding =>
        av.decode(msg.message) match {
          case Success(record) =>
            Map(fields map (field => (field, unwrapValue(record.get(field)))): _*)
          case Failure(e) =>
            throw new IllegalStateException(e.getMessage, e)
        }
      case JsonDecoder =>
        JsonDecoder.decode(msg.message) match {
          case Success(JObject(mapping)) =>
            Map(mapping.map(f => f.name -> unwrap(f.value)): _*) filter { case (k, v) => fields.contains(k) }
          case Success(js) => Map.empty
          case Failure(e) =>
            throw new IllegalStateException(e.getMessage, e)
        }
      case dec =>
        logger.error(s"Incompatible decoder type ${dec.getClass.getName}")
        throw new IllegalStateException(s"Incompatible decoder type ${dec.getClass.getName}")
    }
  }

  private def unwrap(jv: JValue): Any = {
    jv match {
      case JArray(values) => values map unwrap
      case JBool(value) => value
      case JDouble(num) => num
      case JObject(fields) => Map(fields.map(f => f.name -> unwrap(f.value)): _*)
      case JNull => null
      case JString(s) => s
      case unknown =>
        logger.warn(s"Unrecognized typed '$unknown' (${unknown.getClass.getName})")
        null
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