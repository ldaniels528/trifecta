package com.github.ldaniels528.trifecta.sjs.models

import io.scalajs.util.ScalaJsHelper._

import scala.scalajs.js

/**
  * Represents a Message Format Selection
  * @author lawrence.daniels@gmail.com
  */
@js.native
trait FormatSelection extends js.Object {
  var format: String = js.native
}

/**
  * Format Selection
  * @author lawrence.daniels@gmail.com
  */
object FormatSelection {

  def apply(format: String) = {
    val selection = New[FormatSelection]
    selection.format = format
    selection
  }

}
