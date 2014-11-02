package com.ldaniels528.trifecta.decoders

import com.ldaniels528.trifecta.TxConfig
import com.ldaniels528.trifecta.support.messaging.MessageDecoder

import scala.concurrent.ExecutionContext

/**
 * Trifecta Message Codec Factory
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
object MessageCodecs extends AvroCodec {

  def getDecoder(url: String)(implicit config: TxConfig): Option[MessageDecoder[_]] = {
    url match {
      case s if s.startsWith("avro:") => Option(lookupAvroDecoder(s.drop(5)))
      case "json" => Option(JsonDecoder())
      case _ => None
    }
  }

}
