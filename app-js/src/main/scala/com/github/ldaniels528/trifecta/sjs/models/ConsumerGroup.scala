package com.github.ldaniels528.trifecta.sjs.models

import com.github.ldaniels528.scalascript.util.ScalaJsHelper._

import scala.scalajs.js

/**
  * Represents a Set of Consumers grouped by consumer ID
  * @author lawrence.daniels@gmail.com
  */
@js.native
trait ConsumerGroup extends js.Object {
  var consumerId: String = js.native
  var details: js.Array[Consumer] = js.native
}

/**
  * Consumer Group Companion Object
  * @author lawrence.daniels@gmail.com
  */
object ConsumerGroup {

  def apply(consumerId: String, details: js.Array[Consumer] = emptyArray) = {
    val group = makeNew[ConsumerGroup]
    group.consumerId = consumerId
    group.details = details
    group
  }

}