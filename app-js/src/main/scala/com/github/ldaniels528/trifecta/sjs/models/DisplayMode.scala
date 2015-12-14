package com.github.ldaniels528.trifecta.sjs.models

import com.github.ldaniels528.scalascript.util.ScalaJsHelper._

import scala.scalajs.js

/**
  * Display Mode
  * @author lawrence.daniels@gmail.com
  */
@js.native
trait DisplayMode extends js.Object {
  var state: String = js.native
  var avro: String = js.native
}

/**
  * Display Mode Companion Object
  * @author lawrence.daniels@gmail.com
  */
object DisplayMode {
  def apply(state: String, avro: String) = {
    val mode = makeNew[DisplayMode]
    mode.state = state
    mode.avro = avro
    mode
  }
}
