package com.github.ldaniels528.trifecta.messages.codec.avro

import java.io.{File, StringReader}

import com.github.ldaniels528.commons.helpers.OptionHelper._
import com.github.ldaniels528.commons.helpers.ResourceHelper._
import com.github.ldaniels528.trifecta.messages.BinaryMessage
import com.github.ldaniels528.trifecta.messages.codec.MessageDecoder
import com.github.ldaniels528.trifecta.messages.codec.avro.AvroDecoder._
import com.github.ldaniels528.trifecta.messages.logic.Expressions._
import com.github.ldaniels528.trifecta.messages.logic.MessageEvaluation._
import com.github.ldaniels528.trifecta.messages.logic.{Condition, MessageEvaluation}
import com.github.ldaniels528.trifecta.util.FileHelper._
import com.twitter.bijection.Injection
import com.twitter.bijection.avro.GenericAvroCodecs
import net.liftweb.json.JsonAST.JValue
import org.apache.avro.Schema
import org.apache.avro.compiler.idl.Idl
import org.apache.avro.generic.GenericRecord
import org.apache.avro.util.Utf8

import scala.collection.JavaConversions._
import scala.util.{Failure, Success, Try}

/**
  * Avro Message Decoder
  * @author lawrence.daniels@gmail.com
  */
case class AvroDecoder(label: String, schema: Schema) extends MessageDecoder[GenericRecord]
  with JsonTransCoding with MessageEvaluation {
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
    * Decodes the binary message into a JSON object
    * @param message the given binary message
    * @return a decoded message wrapped in a Try-monad
    */
  override def decodeAsJson(message: Array[Byte]): Try[JValue] = {
    import net.liftweb.json._
    decode(message) map (record => parse(record.toString))
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

  override def toString = s"${super.toString}($label)"

}

/**
  * Avro Message Decoder Singleton
  * @author lawrence.daniels@gmail.com
  */
object AvroDecoder {

  def apply(file: File): Option[AvroDecoder] = {
    val name = file.getName
    file.extension.orDie(s"File '$name' has no extension (e.g. '.avsc')") match {
      case ".avsc" =>
        Option(AvroDecoder(name, file.getTextContents))
      case ".avdl" =>
        Option(fromAvDL(name, file.getTextContents))
      case _ => None
    }
  }

  def apply(label: String, schemaString: String): AvroDecoder = {
    val suffix = label.split("\\.").last
    suffix match {
      case "avsc" =>
        new AvroDecoder(label, new Schema.Parser().parse(schemaString))
      case "avdl" =>
        fromAvDL(label, schemaString)
      case _ =>
        throw new IllegalArgumentException(s"Suffix $suffix is not supported - only .avsc or .avdl are recognized")
    }
  }

  private def fromAvDL(label: String, schemaString: String): AvroDecoder = {
    import collection.JavaConverters._

    val name = label.split("\\.").init.mkString.trim
    new StringReader(schemaString) use { rdr =>
      val idlParser = new Idl(rdr)
      val protocol = idlParser.CompilationUnit()
      val schemas = protocol.getTypes.asScala.toSeq
      val mainSchema = schemas.find(_.getName == name) orDie s"File $label does not contain a schema called $name"
      new AvroDecoder(label, mainSchema)
    }
  }

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
