package com.github.ldaniels528.trifecta.io.kafka

import com.github.ldaniels528.trifecta.io.IOCounter
import com.github.ldaniels528.trifecta.io.kafka.KafkaQuerySource._
import com.github.ldaniels528.trifecta.io.zookeeper.ZKProxy
import com.github.ldaniels528.trifecta.messages.BinaryMessage
import com.github.ldaniels528.trifecta.messages.codec.MessageDecoder
import com.github.ldaniels528.trifecta.messages.logic.MessageEvaluation._
import com.github.ldaniels528.trifecta.messages.logic.{Condition, MessageEvaluation}
import com.github.ldaniels528.trifecta.messages.query.{KQLRestrictions, KQLResult, KQLSource}
import org.slf4j.LoggerFactory

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

/**
  * Kafka Query Source
  * @author lawrence.daniels@gmail.com
  */
case class KafkaQuerySource(topic: String, brokers: Seq[Broker], correlationId: Int = 0)(implicit zk: ZKProxy)
  extends KQLSource {
  private lazy val logger = LoggerFactory.getLogger(getClass)

  /**
    * Retrieves messages matching the given criteria up to the given limit.
    * @param fields       the given subset of fields to retrieve
    * @param decoder      the given [[MessageDecoder message decoder]]
    * @param conditions   the given collection of [[Condition conditions]]
    * @param restrictions the given [[KQLRestrictions restrictions]]
    * @param limit        the maximum number of results to return
    * @param counter      the given [[IOCounter I/O counter]]
    * @param ec           the implicit [[ExecutionContext execution context]]
    * @return a promise of a [[KQLResult set of results]]
    */
  override def findMany(fields: Seq[String],
                        decoder: MessageDecoder[_],
                        conditions: Seq[Condition],
                        restrictions: KQLRestrictions,
                        limit: Option[Int],
                        counter: IOCounter)(implicit ec: ExecutionContext): Future[KQLResult] = {
    val startTime = System.currentTimeMillis()
    KafkaMicroConsumer.findMany(topic, brokers, correlationId, conditions, restrictions, limit, counter) map {
      _ map { md =>
        counter.updateWriteCount(1)
        val mapping = evaluate(md, decoder, fields) match {
          case Success(results) => results
          case Failure(e) => Map("__error" -> e.getMessage)
        }
        mapping ++ Map(Partition -> md.partition, Offset -> md.offset)
      }
    } map { values =>
      val elapsedTimeMillis = (System.currentTimeMillis() - startTime).toDouble
      val theFields = if (fields.isAllFields) values.flatMap(_.keys).distinct else fields.toList ::: List(Partition, Offset)
      KQLResult(topic, theFields, values, elapsedTimeMillis)
    }
  }

  /**
    * Decodes the given message
    * @param msg     the given [[BinaryMessage binary message]]
    * @param decoder the given message decoder
    * @return the decoded message
    */
  private def evaluate(msg: BinaryMessage, decoder: MessageDecoder[_], fields: Seq[String]) = {
    decoder match {
      case me: MessageEvaluation => Try(me.evaluate(msg, fields))
      case dec =>
        logger.error(s"Incompatible decoder type ${dec.getClass.getName}")
        Try(throw new IllegalStateException(s"Incompatible decoder type ${dec.getClass.getName}"))
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