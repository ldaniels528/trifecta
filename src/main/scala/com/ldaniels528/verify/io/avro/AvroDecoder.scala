package com.ldaniels528.verify.io.avro

import com.twitter.bijection.Injection
import com.twitter.bijection.avro.GenericAvroCodecs
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord

/**
 * Apache Avro Decoder
 * @author lawrence.daniels@gmail.com
 */
class AvroDecoder(schemaString: String) {
  import scala.util.Try

  val schema = new Schema.Parser().parse(schemaString)
  val converter: Injection[GenericRecord, Array[Byte]] = GenericAvroCodecs.toBinary(schema)

  /**
   * Decodes the byte array based on the avro schema
   */
  def decode(bytes: Array[Byte]): Try[GenericRecord] = converter.invert(bytes)

}