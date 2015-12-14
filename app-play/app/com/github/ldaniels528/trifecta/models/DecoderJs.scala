package com.github.ldaniels528.trifecta.models

import play.api.libs.json.Json

/**
  * Decoder JSON model
  * @author lawrence.daniels@gmail.com
  */
case class DecoderJs(topic: String, schemas: Seq[SchemaJs])

/**
  * Decoder JSON Companion Object
  * @author lawrence.daniels@gmail.com
  */
object DecoderJs {

  implicit val DecoderReads = Json.reads[DecoderJs]

  implicit val DecoderWrites = Json.writes[DecoderJs]

}
