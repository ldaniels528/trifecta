package com.github.ldaniels528.trifecta.sjs.models

import scala.scalajs.js

/**
  * Represents a Broker Group
  * @author lawrence.daniels@gmail.com
  */
@js.native
trait BrokerGroup extends js.Object {
  var host: String = js.native
  var details: js.Array[Broker] = js.native

}
