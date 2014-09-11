package com.ldaniels528.verify.support.avro

import com.ldaniels528.verify.codecs.MessageDecoder
import com.twitter.bijection.Injection
import com.twitter.bijection.avro.GenericAvroCodecs
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord

import scala.util.Try

/**
 * Apache Avro Decoder
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
case class AvroDecoder(schemaString: String) extends MessageDecoder[GenericRecord] {
  val schema = new Schema.Parser().parse(schemaString)
  val converter: Injection[GenericRecord, Array[Byte]] = GenericAvroCodecs.toBinary(schema)

  /**
   * Decodes the binary message (using the Avro schema) into a generic record
   */
  override def decode(message: Array[Byte]): Try[GenericRecord] = converter.invert(message)

  override def toString = schemaString

}