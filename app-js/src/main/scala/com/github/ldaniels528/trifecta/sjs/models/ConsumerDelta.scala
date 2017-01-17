package com.github.ldaniels528.trifecta.sjs.models

import scala.scalajs.js
import scala.scalajs.js.annotation.ScalaJSDefined

/**
  * Consumer Details
  * @author lawrence.daniels@gmail.com
  */
@ScalaJSDefined
class ConsumerDelta(var consumerId: js.UndefOr[String] = js.undefined,
                    var topic: js.UndefOr[String] = js.undefined,
                    var partition: js.UndefOr[Int] = js.undefined,
                    var offset: js.UndefOr[Int] = js.undefined,
                    var topicOffset: js.UndefOr[Int] = js.undefined,
                    var lastModified: js.UndefOr[Double] = js.undefined,
                    var lastModifiedISO: js.UndefOr[js.Date] = js.undefined,
                    var messagesLeft: js.UndefOr[Int] = js.undefined) extends js.Object
