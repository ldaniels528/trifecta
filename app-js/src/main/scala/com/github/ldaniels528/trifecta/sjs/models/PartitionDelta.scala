package com.github.ldaniels528.trifecta.sjs.models

import scala.scalajs.js

/**
  * Represents a Topic Delta
  * @author lawrence.daniels@gmail.com
  * @example {"topic":"test","partition":2,"startOffset":1,"endOffset":0,"messages":0,"totalMessages":0}
  */
@js.native
trait PartitionDelta extends js.Object {
  var topic: js.UndefOr[String] = js.native
  var partition: js.UndefOr[Int] = js.native
  var startOffset: js.UndefOr[Int] = js.native
  var endOffset: js.UndefOr[Int] = js.native
  var messages: Int = js.native
  var totalMessages: Int = js.native
}
