package com.ldaniels528.trifecta.io.avro

import com.ldaniels528.trifecta.io.avro.AvroDecoder._
import com.ldaniels528.trifecta.io.json.JsonTranscoding
import com.ldaniels528.trifecta.messages.MessageDecoder
import com.ldaniels528.trifecta.messages.logic.Expressions._
import com.ldaniels528.trifecta.messages.logic.{Condition, MessageEvaluation}
import com.twitter.bijection.Injection
import com.twitter.bijection.avro.GenericAvroCodecs
import net.liftweb.json._
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.avro.util.Utf8

import scala.util.{Failure, Success, Try}

/**
 * Avro Message Decoder
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
case class AvroDecoder(label: String, schemaString: String) extends MessageDecoder[GenericRecord]
with JsonTranscoding with MessageEvaluation {
  private val schema: Schema = new Schema.Parser().parse(schemaString)
  private val converter: Injection[GenericRecord, Array[Byte]] = GenericAvroCodecs.toBinary(schema)

  /**
   * Compiles the given operation into a condition
   * @param operation the given operation
   * @return a condition
   */
  override def compile(operation: Expression): Condition = {
    operation match {
      case EQ(field, value) => AvroEQ(this, field, value)
      case GE(field, value) => AvroGE(this, field, value)
      case GT(field, value) => AvroGT(this, field, value)
      case LE(field, value) => AvroLE(this, field, value)
      case LT(field, value) => AvroLT(this, field, value)
      case NE(field, value) => AvroNE(this, field, value)
      case _ => throw new IllegalArgumentException(s"Illegal operation '$operation'")
    }
  }

  /**
   * Decodes the binary message (using the Avro schema) into a generic record
   */
  override def decode(message: Array[Byte]): Try[GenericRecord] = converter.invert(message)

  /**
   * Transcodes the given bytes into JSON
   * @param bytes the given byte array
   * @return a JSON value
   */
  override def toJSON(bytes: Array[Byte]): Try[JValue] = decode(bytes) map (_.toString) map parse

  override def toString = s"${super.toString}($label)"

}

/**
 * Avro Decoder Singleton
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
object AvroDecoder {

  /**
   * Avro Field-Value Equality Condition
   * @author Lawrence Daniels <lawrence.daniels@gmail.com>
   */
  case class AvroEQ(decoder: AvroDecoder, field: String, value: String) extends Condition {
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
   * @author Lawrence Daniels <lawrence.daniels@gmail.com>
   */
  case class AvroGT(decoder: AvroDecoder, field: String, value: String) extends Condition {
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
   * @author Lawrence Daniels <lawrence.daniels@gmail.com>
   */
  case class AvroGE(decoder: AvroDecoder, field: String, value: String) extends Condition {
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
   * Avro Field-Value Less-Than Condition
   * @author Lawrence Daniels <lawrence.daniels@gmail.com>
   */
  case class AvroLT(decoder: AvroDecoder, field: String, value: String) extends Condition {
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
   * Avro Field-Value Less-Than-Or-Equal Condition
   * @author Lawrence Daniels <lawrence.daniels@gmail.com>
   */
  case class AvroLE(decoder: AvroDecoder, field: String, value: String) extends Condition {
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
   * Avro Field-Value Inequality Condition
   * @author Lawrence Daniels <lawrence.daniels@gmail.com>
   */
  case class AvroNE(decoder: AvroDecoder, field: String, value: String) extends Condition {
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