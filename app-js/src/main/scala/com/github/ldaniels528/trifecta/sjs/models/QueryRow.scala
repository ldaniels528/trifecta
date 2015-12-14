package com.github.ldaniels528.trifecta.sjs.models

import scala.scalajs.js

/**
  * Query Row
  * @author lawrence.daniels@gmail.com
  */
@js.native
trait QueryRow extends js.Object {
  var topic: js.UndefOr[String] = js.native
  var __offset: js.UndefOr[Int] = js.native
  var __partition: js.UndefOr[Int] = js.native
  var values: js.Dictionary[Any] = js.native

}
