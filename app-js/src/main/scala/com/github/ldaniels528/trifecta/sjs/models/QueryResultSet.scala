package com.github.ldaniels528.trifecta.sjs.models

import scala.scalajs.js
import scala.scalajs.js.annotation.ScalaJSDefined

/**
  * Query Result Set
  * @author lawrence.daniels@gmail.com
  */
@ScalaJSDefined
trait QueryResultSet extends js.Object {
  var topic: js.UndefOr[String]
  var columns: js.UndefOr[js.Array[String]]
  var rows: js.UndefOr[js.Array[QueryRow]]
}
