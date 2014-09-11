package com.ldaniels528.verify.modules.avro

import com.ldaniels528.verify.support.avro.AvroDecoder
import com.ldaniels528.verify.support.kafka.Condition
import org.slf4j.LoggerFactory

import scala.util.{Failure, Success}

/**
 * Avro Conditions
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
object AvroConditions {
  private lazy val logger = LoggerFactory.getLogger(getClass)

  /**
   * Avro Field-Value Equality Condition
   */
  case class AvroEQ(decoder: AvroDecoder, field: String, value: String) extends Condition {
    override def satisfies(message: Array[Byte], key: Option[Array[Byte]]): Boolean = {
      decoder.decode(message) match {
        case Success(record) =>
          record.get(field) match {
            case v: java.lang.Number => v.doubleValue() == value.toDouble
            case s: String => s == value
            case x =>
              throw new IllegalStateException(s"Value '$x' (${Option(x).map(_.getClass.getName).orNull}) for field '$field' was not recognized")
          }
        case Failure(e) => false
      }
    }
  }

  /**
   * Avro Field-Value Greater-Than Condition
   */
  case class AvroGreater(decoder: AvroDecoder, field: String, value: String) extends Condition {
    override def satisfies(message: Array[Byte], key: Option[Array[Byte]]): Boolean = {
      decoder.decode(message) match {
        case Success(record) =>
          record.get(field) match {
            case v: java.lang.Number => v.doubleValue() > value.toDouble
            case s: String => s > value
            case x => throw new IllegalStateException(s"Value '$x' for field '$field' was not recognized")
          }
        case Failure(e) => false
      }
    }
  }

  /**
   * Avro Field-Value Greater-Than-Or-Equal Condition
   */
  case class AvroGreaterOrEQ(decoder: AvroDecoder, field: String, value: String) extends Condition {
    override def satisfies(message: Array[Byte], key: Option[Array[Byte]]): Boolean = {
      decoder.decode(message) match {
        case Success(record) =>
          record.get(field) match {
            case v: java.lang.Number => v.doubleValue() >= value.toDouble
            case s: String => s >= value
            case x => throw new IllegalStateException(s"Value '$x' for field '$field' was not recognized")
          }
        case Failure(e) => false
      }
    }
  }

  /**
   * Avro Field-Value Less-Than Condition
   */
  case class AvroLesser(decoder: AvroDecoder, field: String, value: String) extends Condition {
    override def satisfies(message: Array[Byte], key: Option[Array[Byte]]): Boolean = {
      decoder.decode(message) match {
        case Success(record) =>
          record.get(field) match {
            case v: java.lang.Number => v.doubleValue() < value.toDouble
            case s: String => s < value
            case x => throw new IllegalStateException(s"Value '$x' for field '$field' was not recognized")
          }
        case Failure(e) => false
      }
    }
  }

  /**
   * Avro Field-Value Less-Than-Or-Equal Condition
   */
  case class AvroLesserOrEQ(decoder: AvroDecoder, field: String, value: String) extends Condition {
    override def satisfies(message: Array[Byte], key: Option[Array[Byte]]): Boolean = {
      decoder.decode(message) match {
        case Success(record) =>
          record.get(field) match {
            case v: java.lang.Number => v.doubleValue() <= value.toDouble
            case s: String => s <= value
            case x => throw new IllegalStateException(s"Value '$x' for field '$field' was not recognized")
          }
        case Failure(e) => false
      }
    }
  }

  /**
   * Avro Field-Value Inequality Condition
   */
  case class AvroNotEQ(decoder: AvroDecoder, field: String, value: String) extends Condition {
    override def satisfies(message: Array[Byte], key: Option[Array[Byte]]): Boolean = {
      decoder.decode(message) match {
        case Success(record) =>
          record.get(field) match {
            case v: java.lang.Number => v.doubleValue() != value.toDouble
            case s: String => s != value
            case x => throw new IllegalStateException(s"Value '$x' for field '$field' was not recognized")
          }
        case Failure(e) => false
      }
    }
  }

}
