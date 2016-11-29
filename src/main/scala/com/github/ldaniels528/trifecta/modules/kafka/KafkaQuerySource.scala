package com.github.ldaniels528.trifecta.modules.kafka

import com.github.ldaniels528.trifecta.io.AsyncIO.IOCounter
import com.github.ldaniels528.trifecta.messages.logic.{Condition, MessageEvaluation}
import com.github.ldaniels528.trifecta.messages.query.{KQLResult, KQLSource}
import com.github.ldaniels528.trifecta.messages.{BinaryMessage, MessageDecoder}
import com.github.ldaniels528.trifecta.modules.kafka.KafkaQuerySource._
import com.github.ldaniels528.trifecta.modules.zookeeper.ZKProxy
import org.slf4j.LoggerFactory

import scala.concurrent.{ExecutionContext, Future}

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
                       counter: IOCounter)(implicit ec: ExecutionContext): Future[KQLResult] = {
    val startTime = System.nanoTime()
    KafkaMicroConsumer.findAll(topic, brokers, correlationId, conditions, limit, counter) map {
      _ map { md =>
        counter.updateWriteCount(1)
        val mapping = decodeMessage(md, decoder, fields)
        mapping ++ Map(Partition -> md.partition, Offset -> md.offset)
      }
    } map { values =>
      val elapsedTimeMillis = (System.nanoTime() - startTime).toDouble / 1e9
      val theFields = if (fields.contains("*")) values.flatMap(_.keys).distinct else fields.toList ::: List(Partition, Offset)
      KQLResult(topic, theFields, values, elapsedTimeMillis)
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
      case me: MessageEvaluation => me.evaluate(msg, fields)
      case dec =>
        logger.error(s"Incompatible decoder type ${dec.getClass.getName}")
        throw new IllegalStateException(s"Incompatible decoder type ${dec.getClass.getName}")
    }
  }

}

/**
  * Kafka Query Source Companion
  * @author lawrence.daniels@gmail.com
  */
object KafkaQuerySource {
  val Partition = "__partition"
  val Offset = "__offset"

}