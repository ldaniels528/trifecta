package com.github.ldaniels528.trifecta.sjs.models

import scala.scalajs.js
import scala.scalajs.js.annotation.ScalaJSDefined

/**
  * Consumer Owner
  * @author lawrence.daniels@gmail.com
  */
@ScalaJSDefined
class ConsumerOwner(var groupId: js.UndefOr[String] = js.undefined,
                    var topic: js.UndefOr[String] = js.undefined,
                    var partition: js.UndefOr[Int] = js.undefined,
                    var threadId: js.UndefOr[String] = js.undefined) extends js.Object