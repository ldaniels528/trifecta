package com.github.ldaniels528.trifecta.sjs.models

import com.github.ldaniels528.scalascript.util.ScalaJsHelper._

import scala.scalajs.js

/**
  * Represents a Decoder Schema
  * @author lawrence.daniels@gmail.com
  */
@js.native
trait DecoderSchema extends js.Object {
  var name: js.UndefOr[String] = js.native
  var decoder: js.UndefOr[Decoder] = js.native
  var topic: js.UndefOr[String] = js.native
  var schemaString: js.UndefOr[String] = js.native

  // ui-related properties
  var editMode: js.UndefOr[Boolean] = js.native
  var error: js.UndefOr[String] = js.native
  var modified: js.UndefOr[Boolean] = js.native
  var newSchema: js.UndefOr[Boolean] = js.native
  var originalSchemaString: js.UndefOr[String] = js.native
  var processing: js.UndefOr[Boolean] = js.native

}

/**
  * Decoder Schema Companion Object
  * @author lawrence.daniels@gmail.com
  */
object DecoderSchema {

  def apply(name: js.UndefOr[String] = js.undefined,
            decoder: js.UndefOr[Decoder] = js.undefined,
            topic: js.UndefOr[String] = js.undefined,
            schemaString: js.UndefOr[String] = "") = {
    val schema = makeNew[DecoderSchema]
    schema.decoder = decoder
    schema.topic = topic
    schema.name = name
    schema.originalSchemaString = schemaString
    schema.schemaString = schemaString
    schema.editMode = true
    schema.modified = true
    schema.newSchema = true
    schema
  }

}
