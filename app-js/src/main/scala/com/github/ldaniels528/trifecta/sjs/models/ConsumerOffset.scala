package com.github.ldaniels528.trifecta.sjs.models

import scala.scalajs.js
import scala.scalajs.js.annotation.ScalaJSDefined

/**
  * Consumer Offset
  * @author lawrence.daniels@gmail.com
  */
@ScalaJSDefined
class ConsumerOffset(var topic: js.UndefOr[String] = js.undefined,
                     var partition: js.UndefOr[Int] = js.undefined,
                     var offset: js.UndefOr[Double] = js.undefined,
                     var topicStartOffset: js.UndefOr[Double] = js.undefined,
                     var topicEndOffset: js.UndefOr[Double] = js.undefined,
                     var messages: js.UndefOr[Double] = js.undefined,
                     var lastModifiedTime: js.UndefOr[Double] = js.undefined) extends js.Object {

  var expanded: js.UndefOr[Boolean] = js.undefined
  var deltaC: js.UndefOr[Double] = js.undefined
  var deltaT: js.UndefOr[Double] = js.undefined
}