package com.ldaniels528.verify.support.avro

import com.ldaniels528.verify.support.messaging.logic.Condition

import scala.util.{Failure, Success}

/**
 * Avro Conditions
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
object AvroConditions {

  /**
   * Avro Field-Value Equality Condition
   * @author Lawrence Daniels <lawrence.daniels@gmail.com>
   */
  case class AvroEQ(decoder: AvroDecoder, field: String, value: String) extends Condition {
    override def satisfies(message: Array[Byte], key: Array[Byte]): Boolean = {
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
   * @author Lawrence Daniels <lawrence.daniels@gmail.com>
   */
  case class AvroGT(decoder: AvroDecoder, field: String, value: String) extends Condition {
    override def satisfies(message: Array[Byte], key: Array[Byte]): Boolean = {
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
   * @author Lawrence Daniels <lawrence.daniels@gmail.com>
   */
  case class AvroGE(decoder: AvroDecoder, field: String, value: String) extends Condition {
    override def satisfies(message: Array[Byte], key: Array[Byte]): Boolean = {
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
   * @author Lawrence Daniels <lawrence.daniels@gmail.com>
   */
  case class AvroLT(decoder: AvroDecoder, field: String, value: String) extends Condition {
    override def satisfies(message: Array[Byte], key: Array[Byte]): Boolean = {
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
   * @author Lawrence Daniels <lawrence.daniels@gmail.com>
   */
  case class AvroLE(decoder: AvroDecoder, field: String, value: String) extends Condition {
    override def satisfies(message: Array[Byte], key: Array[Byte]): Boolean = {
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
   * @author Lawrence Daniels <lawrence.daniels@gmail.com>
   */
  case class AvroNE(decoder: AvroDecoder, field: String, value: String) extends Condition {
    override def satisfies(message: Array[Byte], key: Array[Byte]): Boolean = {
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
