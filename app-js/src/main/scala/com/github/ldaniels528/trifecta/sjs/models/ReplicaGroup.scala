package com.github.ldaniels528.trifecta.sjs.models

import scala.scalajs.js

/**
  * Represents a Replica Group
  * @author lawrence.daniels@gmail.com
  */
@js.native
trait ReplicaGroup extends js.Object {
  var inSyncPct: js.UndefOr[Int] = js.native
  var partition: js.UndefOr[Int] = js.native
  var replicas: js.UndefOr[js.Array[ReplicaBroker]] = js.native

}
