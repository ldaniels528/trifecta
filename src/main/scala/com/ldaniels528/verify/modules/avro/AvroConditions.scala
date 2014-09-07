package com.ldaniels528.verify.modules.avro

import com.ldaniels528.verify.modules.kafka.KafkaStreamingConsumer.Condition
import kafka.message.MessageAndMetadata

import scala.util.{Failure, Success}

/**
 * Avro Conditions
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
object AvroConditions {

  /**
   * Avro Field-Value Equality Condition
   */
  case class AvroEQ(decoder: AvroDecoder, field: String, value: String) extends Condition {
    override def satisfies(mam: MessageAndMetadata[Array[Byte], Array[Byte]]): Boolean = {
      decoder.decode(mam.message()) match {
        case Success(record) =>
          val myValue = record.get(field)
          myValue match {
            case v: java.lang.Number => v.doubleValue() == value.toDouble
            case s: String => s == value
            case _ => false
          }
        case Failure(e) => false
      }
    }
  }

  /**
   * Avro Field-Value Greater-Than Condition
   */
  case class AvroGreater(decoder: AvroDecoder, field: String, value: String) extends Condition {
    override def satisfies(mam: MessageAndMetadata[Array[Byte], Array[Byte]]): Boolean = {
      decoder.decode(mam.message()) match {
        case Success(record) =>
          val myValue = record.get(field)
          myValue match {
            case v: java.lang.Number => v.doubleValue() > value.toDouble
            case s: String => s > value
            case _ => false
          }
        case Failure(e) => false
      }
    }
  }

  /**
   * Avro Field-Value Greater-Than-Or-Equal Condition
   */
  case class AvroGreaterOrEQ(decoder: AvroDecoder, field: String, value: String) extends Condition {
    override def satisfies(mam: MessageAndMetadata[Array[Byte], Array[Byte]]): Boolean = {
      decoder.decode(mam.message()) match {
        case Success(record) =>
          val myValue = record.get(field)
          myValue match {
            case v: java.lang.Number => v.doubleValue() >= value.toDouble
            case s: String => s >= value
            case _ => false
          }
        case Failure(e) => false
      }
    }
  }

  /**
   * Avro Field-Value Less-Than Condition
   */
  case class AvroLesser(decoder: AvroDecoder, field: String, value: String) extends Condition {
    override def satisfies(mam: MessageAndMetadata[Array[Byte], Array[Byte]]): Boolean = {
      decoder.decode(mam.message()) match {
        case Success(record) =>
          val myValue = record.get(field)
          myValue match {
            case v: java.lang.Number => v.doubleValue() < value.toDouble
            case s: String => s < value
            case _ => false
          }
        case Failure(e) => false
      }
    }
  }

  /**
   * Avro Field-Value Less-Than-Or-Equal Condition
   */
  case class AvroLesserOrEQ(decoder: AvroDecoder, field: String, value: String) extends Condition {
    override def satisfies(mam: MessageAndMetadata[Array[Byte], Array[Byte]]): Boolean = {
      decoder.decode(mam.message()) match {
        case Success(record) =>
          val myValue = record.get(field)
          myValue match {
            case v: java.lang.Number => v.doubleValue() <= value.toDouble
            case s: String => s <= value
            case _ => false
          }
        case Failure(e) => false
      }
    }
  }

  /**
   * Avro Field-Value Inequality Condition
   */
  case class AvroNotEQ(decoder: AvroDecoder, field: String, value: String) extends Condition {
    override def satisfies(mam: MessageAndMetadata[Array[Byte], Array[Byte]]): Boolean = {
      decoder.decode(mam.message()) match {
        case Success(record) =>
          val myValue = record.get(field)
          myValue match {
            case v: java.lang.Number => v.doubleValue() != value.toDouble
            case s: String => s != value
            case _ => false
          }
        case Failure(e) => false
      }
    }
  }

}
