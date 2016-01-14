package com.github.ldaniels528.trifecta.sjs.models

import scala.scalajs.js

/**
  * Represents a Decoder
  * @author lawrence.daniels@gmail.com
  */
@js.native
trait Decoder extends js.Object {
  var topic: js.UndefOr[String] = js.native
  var schemas: js.UndefOr[js.Array[DecoderSchema]] = js.native

  // ui-related properties
  var decoderExpanded: js.UndefOr[Boolean] = js.native
  var loading: js.UndefOr[Boolean] = js.native
}
