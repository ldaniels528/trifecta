package com.github.ldaniels528.trifecta.sjs.models

import scala.scalajs.js

/**
  * Represents a Simple Broker object
  * @author lawrence.daniels@gmail.com
  */
@js.native
trait Broker extends js.Object {
  var host: js.UndefOr[String] = js.native
  var port: js.UndefOr[Int] = js.native
  var brokerId: js.UndefOr[Int] = js.native
  var jmx_port: js.UndefOr[Int] = js.native
  var timestamp: js.UndefOr[js.Date]
  var version: js.UndefOr[Int] = js.native

}
