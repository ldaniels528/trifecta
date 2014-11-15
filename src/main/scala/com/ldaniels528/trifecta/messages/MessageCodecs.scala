package com.ldaniels528.trifecta.messages

import com.ldaniels528.trifecta.TxConfig
import com.ldaniels528.trifecta.io.avro.AvroCodec
import com.ldaniels528.trifecta.io.gzip.GzipCodec
import com.ldaniels528.trifecta.io.json.JsonDecoder

/**
 * Trifecta Message Codec Factory
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
object MessageCodecs extends AvroCodec {

  def getDecoder(url: String)(implicit config: TxConfig): Option[MessageDecoder[_]] = {
    url match {
      case s if s.startsWith("avro:") => Option(lookupAvroDecoder(s.drop(5)))
      case "json" => Option(JsonDecoder())
      case "gzip" => Option(GzipCodec())
      case _ => None
    }
  }

  def getEncoder(url: String)(implicit config: TxConfig): Option[MessageEncoder[_]] = {
    url match {
      case "gzip" => Option(GzipCodec())
      case _ => None
    }
  }

}
