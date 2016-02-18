package com.github.ldaniels528.trifecta.sjs.models

import com.github.ldaniels528.scalascript.util.ScalaJsHelper._

import scala.scalajs.js

/**
  * Decoder Schema
  * @author lawrence.daniels@gmail.com
  */
@js.native
trait DecoderSchema extends js.Object {
  var topic: js.UndefOr[String] = js.native
  var name: js.UndefOr[String] = js.native
  var schemaString: js.UndefOr[String] = js.native

  // ui-related properties
  var decoder: js.UndefOr[Decoder] = js.native
  var error: js.UndefOr[String] = js.native
  var processing: js.UndefOr[Boolean] = js.native
}

/**
  * Decoder Schema Companion Object
  * @author lawrence.daniels@gmail.com
  */
object DecoderSchema {

  def apply(decoder: js.UndefOr[Decoder],
            topic: js.UndefOr[String],
            name: js.UndefOr[String],
            schemaString: js.UndefOr[String] = js.undefined) = {
    val schema = makeNew[DecoderSchema]
    schema.decoder = decoder
    schema.topic = topic
    schema.name = name
    schema.schemaString = schemaString
    schema
  }

}