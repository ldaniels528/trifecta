package com.ldaniels528.trifecta.support.avro

import com.ldaniels528.trifecta.support.avro.AvroConditions._
import com.ldaniels528.trifecta.support.messaging.MessageDecoder
import com.ldaniels528.trifecta.support.messaging.logic.Operations._
import com.ldaniels528.trifecta.support.messaging.logic.{Condition, MessageEvaluation}
import com.twitter.bijection.Injection
import com.twitter.bijection.avro.GenericAvroCodecs
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord

import scala.util.Try

/**
 * Apache Avro Decoder
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
case class AvroDecoder(label: String, schemaString: String) extends MessageDecoder[GenericRecord]
with MessageEvaluation {
  val schema = new Schema.Parser().parse(schemaString)
  val converter: Injection[GenericRecord, Array[Byte]] = GenericAvroCodecs.toBinary(schema)

  override def compile(operation: Operation): Condition = {
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

  override def toString = s"${super.toString}($label)"

}