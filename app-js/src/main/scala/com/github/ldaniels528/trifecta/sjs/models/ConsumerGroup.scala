package com.github.ldaniels528.trifecta.sjs.models

import org.scalajs.nodejs.util.ScalaJsHelper._

import scala.scalajs.js
import scala.scalajs.js.annotation.ScalaJSDefined

/**
  * Represents a Set of Consumers grouped by consumer ID
  * @author lawrence.daniels@gmail.com
  */
@ScalaJSDefined
class ConsumerGroup(var consumerId: String,
                    var details: js.Array[Consumer] = emptyArray) extends js.Object
