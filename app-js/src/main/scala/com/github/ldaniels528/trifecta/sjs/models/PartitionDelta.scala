package com.github.ldaniels528.trifecta.sjs.models

import scala.scalajs.js
import scala.scalajs.js.annotation.ScalaJSDefined

/**
  * Represents a Topic Delta
  * @example {{{ {"topic":"test","partition":2,"startOffset":1,"endOffset":0,"messages":0,"totalMessages":0} }}}
  * @author lawrence.daniels@gmail.com
  */
@ScalaJSDefined
class PartitionDelta(var topic: js.UndefOr[String] = js.undefined,
                     var partition: js.UndefOr[Int] = js.undefined,
                     var startOffset: js.UndefOr[Int] = js.undefined,
                     var endOffset: js.UndefOr[Int] = js.undefined,
                     var messages: js.UndefOr[Int] = js.undefined,
                     var totalMessages: js.UndefOr[Int] = js.undefined) extends js.Object
