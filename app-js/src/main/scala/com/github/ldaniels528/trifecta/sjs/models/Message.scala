package com.github.ldaniels528.trifecta.sjs.models

import scala.scalajs.js

/**
  * Represents a Message
  * @author lawrence.daniels@gmail.com
  */
@js.native
trait Message extends js.Object {
  var offset: js.UndefOr[Int] = js.native
  var partition: js.UndefOr[Int] = js.native
  var `type`: js.UndefOr[String] = js.native
  var payload: js.UndefOr[js.Object] = js.native

}
