package com.github.ldaniels528.trifecta.sjs.models

import scala.scalajs.js
import scala.scalajs.js.annotation.ScalaJSDefined

/**
  * Consumer Thread
  * @author lawrence.daniels@gmail.com
  */
@ScalaJSDefined
class ConsumerThread(var groupId: js.UndefOr[String] = js.undefined,
                     var threadId: js.UndefOr[String] = js.undefined,
                     var version: js.UndefOr[Int] = js.undefined,
                     var topic: js.UndefOr[String] = js.undefined,
                     var timestamp: js.UndefOr[String] = js.undefined,
                     var timestampISO: js.UndefOr[String] = js.undefined) extends js.Object