package com.ldaniels528.trifecta.decoders

import com.ldaniels528.trifecta.support.messaging.MessageDecoder

/**
 * Trifecta Message Codec Factory
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
object MessageCodecs extends AvroCodec {

  def getDecoder(url: String): Option[MessageDecoder[_]] = {
    url match {
      case s if s.startsWith("avro:") => Option(lookupAvroDecoder(s.drop(5)))
      case "json" => Option(JsonDecoder())
      case _ => None
    }
  }

}
