package com.github.ldaniels528.trifecta.io.avro

import com.github.ldaniels528.trifecta.io.avro.AvroCodec._
import com.github.ldaniels528.trifecta.io.json.JsonTranscoding
import com.github.ldaniels528.trifecta.messages.logic.Expressions._
import com.github.ldaniels528.trifecta.messages.logic.{Condition, MessageEvaluation}
import com.twitter.bijection.Injection
import com.twitter.bijection.avro.GenericAvroCodecs
import net.liftweb.json._
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord

import scala.util.Try

/**
 * Avro Message Decoder
 * @author lawrence.daniels@gmail.com
 */
case class AvroDecoder(label: String, schema: Schema) extends AvroMessageDecoding
with JsonTranscoding with MessageEvaluation {
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
 * Avro Message Decoder Singleton
 * @author lawrence.daniels@gmail.com
 */
object AvroDecoder {

  def apply(label: String, schemaString: String) = new AvroDecoder(label, new Schema.Parser().parse(schemaString))

}