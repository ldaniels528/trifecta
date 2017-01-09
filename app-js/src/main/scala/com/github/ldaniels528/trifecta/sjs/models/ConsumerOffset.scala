package com.github.ldaniels528.trifecta.sjs.models

import scala.scalajs.js
import scala.scalajs.js.annotation.ScalaJSDefined

/**
  * Consumer Offset
  * @author lawrence.daniels@gmail.com
  */
@ScalaJSDefined
class ConsumerOffset(var consumerId: js.UndefOr[String] = js.undefined,
                     var topic: js.UndefOr[String] = js.undefined,
                     var partition: js.UndefOr[Int] = js.undefined,
                     var offset: js.UndefOr[Int] = js.undefined,
                     var lastModifiedTime: js.UndefOr[Double] = js.undefined) extends js.Object
