package com.github.ldaniels528.trifecta.sjs.models

import scala.scalajs.js

/**
  * Query Row
  * @author lawrence.daniels@gmail.com
  */
@js.native
trait QueryRow extends js.Object {
  var topic: js.UndefOr[String] = js.native
  var __offset: js.UndefOr[Double] = js.native
  var __partition: js.UndefOr[Int] = js.native
  var __error: js.UndefOr[String] = js.native
  // and other columns ...
}

/**
  * Query Row Companion
  * @author lawrence.daniels@gmail.com
  */
object QueryRow {
  val ColumnsWithoutError = js.Array("__partition", "__offset", "__key")
  val ColumnsWithError =  js.Array("__partition", "__offset", "__key", "__error")

}