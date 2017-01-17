package com.github.ldaniels528.trifecta.messages.codec.avro

import com.github.ldaniels528.trifecta.messages.BinaryMessage
import com.github.ldaniels528.trifecta.messages.codec.MessageDecoder
import com.github.ldaniels528.trifecta.messages.codec.avro.AvroMessageEvaluation._
import com.github.ldaniels528.trifecta.messages.logic.Expressions._
import com.github.ldaniels528.trifecta.messages.logic.MessageEvaluation._
import com.github.ldaniels528.trifecta.messages.logic.{Condition, ConditionCompiler, MessageEvaluation}
import org.apache.avro.generic.GenericRecord
import org.apache.avro.util.Utf8

import scala.collection.JavaConversions._
import scala.util.{Failure, Success}

/**
  * Avro Message Evaluation
  * @author lawrence.daniels@gmail.com
  */
trait AvroMessageEvaluation extends MessageEvaluation {
  self: MessageDecoder[GenericRecord] =>

  /**
    * Compiles the given expression into a condition
    * @param expression the given expression
    * @return a condition
    */
  override def compile(expression: Expression): Condition = {
    expression match {
      case EQ(field, value) => AvroEQ(this, field, value)
      case GE(field, value) => AvroGE(this, field, value)
      case GT(field, value) => AvroGT(this, field, value)
      case LE(field, value) => AvroLE(this, field, value)
      case LIKE(field, pattern) => AvroLIKE(this, field, pattern)
      case LT(field, value) => AvroLT(this, field, value)
      case MATCHES(field, pattern) => AvroMATCHES(this, field, pattern)
      case NE(field, value) => AvroNE(this, field, value)
      case _ => throw new IllegalArgumentException(s"Illegal expression '$expression'")
    }
  }

  /**
    * Evaluates the message; returning the resulting field and values
    * @param msg    the given [[BinaryMessage binary message]]
    * @param fields the given subset of fields to return
    * @return the mapping of fields and values
    */
  override def evaluate(msg: BinaryMessage, fields: Seq[String]): Map[String, Any] = {
    decode(msg.message) match {
      case Success(record) =>
        if (fields.isAllFields) {
          val allFields = record.getSchema.getFields.map(_.name())
          Map(allFields map (field => field -> unwrapValue(record.get(field))): _*)
        } else
          Map(fields map (field => field -> unwrapValue(record.get(field))): _*)
      case Failure(e) =>
        throw new IllegalStateException("Malformed Avro message", e)
    }
  }

  private def unwrapValue(value: AnyRef) = {
    value match {
      case u: Utf8 => u.toString
      case x => x
    }
  }

}

/**
  * Avro Message Evaluation Companion
  * @author lawrence.daniels@gmail.com
  */
object AvroMessageEvaluation {

  /**
    * Avro Field-Value Equality Condition
    * @author lawrence.daniels@gmail.com
    */
  case class AvroEQ(decoder: MessageDecoder[GenericRecord], field: String, value: String) extends Condition {
    override def satisfies(message: Array[Byte], key: Array[Byte]): Boolean = {
      decoder.decode(message) match {
        case Success(record) =>
          record.get(field) match {
            case v if v == null => value == null
            case v: Utf8 => v.toString == value
            case v: java.lang.Number => v.doubleValue() == value.toDouble
            case s: String => s == value
            case x =>
              throw new IllegalStateException(s"Value '$x' (${Option(x).map(_.getClass.getName).orNull}) for field '$field' was not recognized")
          }
        case Failure(e) => false
      }
    }

    override def toString = s"$field == '$value'"
  }

  /**
    * Avro Field-Value Greater-Than Condition
    * @author lawrence.daniels@gmail.com
    */
  case class AvroGT(decoder: MessageDecoder[GenericRecord], field: String, value: String) extends Condition {
    override def satisfies(message: Array[Byte], key: Array[Byte]): Boolean = {
      decoder.decode(message) match {
        case Success(record) =>
          record.get(field) match {
            case null => false
            case v: Utf8 => v.toString == value
            case v: java.lang.Number => v.doubleValue() > value.toDouble
            case s: String => s > value
            case x => throw new IllegalStateException(s"Value '$x' for field '$field' was not recognized")
          }
        case Failure(e) => false
      }
    }

    override def toString = s"$field > $value'"
  }

  /**
    * Avro Field-Value Greater-Than-Or-Equal Condition
    * @author lawrence.daniels@gmail.com
    */
  case class AvroGE(decoder: MessageDecoder[GenericRecord], field: String, value: String) extends Condition {
    override def satisfies(message: Array[Byte], key: Array[Byte]): Boolean = {
      decoder.decode(message) match {
        case Success(record) =>
          record.get(field) match {
            case null => false
            case v: Utf8 => v.toString == value
            case v: java.lang.Number => v.doubleValue() >= value.toDouble
            case s: String => s >= value
            case x => throw new IllegalStateException(s"Value '$x' for field '$field' was not recognized")
          }
        case Failure(e) => false
      }
    }

    override def toString = s"$field >= '$value'"
  }

  /**
    * Avro Field-Value Less-Than-Or-Equal Condition
    * @author lawrence.daniels@gmail.com
    */
  case class AvroLE(decoder: MessageDecoder[GenericRecord], field: String, value: String) extends Condition {
    override def satisfies(message: Array[Byte], key: Array[Byte]): Boolean = {
      decoder.decode(message) match {
        case Success(record) =>
          record.get(field) match {
            case null => false
            case v: Utf8 => v.toString == value
            case v: java.lang.Number => v.doubleValue() <= value.toDouble
            case s: String => s <= value
            case x => throw new IllegalStateException(s"Value '$x' for field '$field' was not recognized")
          }
        case Failure(e) => false
      }
    }

    override def toString = s"$field <= '$value'"
  }

  /**
    * Avro Field-Value Like Condition
    * @author lawrence.daniels@gmail.com
    */
  case class AvroLIKE(decoder: MessageDecoder[GenericRecord], field: String, pattern: String) extends Condition {

    import ConditionCompiler._

    override def satisfies(message: Array[Byte], key: Array[Byte]): Boolean = {
      decoder.decode(message) match {
        case Success(record) =>
          record.get(field) match {
            case null => false
            case v: Utf8 => v.toString.like(pattern)
            case s: String => s.like(pattern)
            case x => x.toString.like(pattern)
          }
        case Failure(e) => false
      }
    }

    override def toString = s"$field < '$pattern'"
  }

  /**
    * Avro Field-Value Less-Than Condition
    * @author lawrence.daniels@gmail.com
    */
  case class AvroLT(decoder: MessageDecoder[GenericRecord], field: String, value: String) extends Condition {
    override def satisfies(message: Array[Byte], key: Array[Byte]): Boolean = {
      decoder.decode(message) match {
        case Success(record) =>
          record.get(field) match {
            case null => false
            case v: Utf8 => v.toString == value
            case v: java.lang.Number => v.doubleValue() < value.toDouble
            case s: String => s < value
            case x => throw new IllegalStateException(s"Value '$x' for field '$field' was not recognized")
          }
        case Failure(e) => false
      }
    }

    override def toString = s"$field < '$value'"
  }

  /**
    * Avro Field-Value Matches Condition
    * @author lawrence.daniels@gmail.com
    */
  case class AvroMATCHES(decoder: MessageDecoder[GenericRecord], field: String, pattern: String) extends Condition {
    override def satisfies(message: Array[Byte], key: Array[Byte]): Boolean = {
      decoder.decode(message) match {
        case Success(record) =>
          record.get(field) match {
            case null => false
            case v: Utf8 => v.toString.matches(pattern)
            case s: String => s.matches(pattern)
            case x => x.toString.matches(pattern)
          }
        case Failure(e) => false
      }
    }

    override def toString = s"$field < '$pattern'"
  }

  /**
    * Avro Field-Value Inequality Condition
    * @author lawrence.daniels@gmail.com
    */
  case class AvroNE(decoder: MessageDecoder[GenericRecord], field: String, value: String) extends Condition {
    override def satisfies(message: Array[Byte], key: Array[Byte]): Boolean = {
      decoder.decode(message) match {
        case Success(record) =>
          record.get(field) match {
            case v if v == null => value != null
            case v: Utf8 => v.toString == value
            case v: java.lang.Number => v.doubleValue() != value.toDouble
            case s: String => s != value
            case x => throw new IllegalStateException(s"Value '$x' for field '$field' was not recognized")
          }
        case Failure(e) => false
      }
    }

    override def toString = s"$field != '$value'"
  }

}