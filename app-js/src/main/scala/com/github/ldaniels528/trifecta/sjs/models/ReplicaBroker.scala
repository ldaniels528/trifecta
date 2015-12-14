package com.github.ldaniels528.trifecta.sjs.models

import scala.scalajs.js

/**
  * Represents a Replica Broker
  * @author lawrence.daniels@gmail.com
  */
@js.native
trait ReplicaBroker extends Broker {

  // ui-specific properties
  var expanded: js.UndefOr[Boolean] = js.native
  var inSync: js.UndefOr[Boolean] = js.native


}
