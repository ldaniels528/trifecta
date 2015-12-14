package com.github.ldaniels528.trifecta.sjs.models

import com.github.ldaniels528.scalascript.util.ScalaJsHelper._

import scala.scalajs.js

/**
  * Sampling Action
  * @author lawrence.daniels@gmail.com
  */
@js.native
trait SamplingAction extends js.Object {
  var action: String = js.native
  var topic: String = js.native
  var partitions: js.Array[Int] = js.native

}

/**
  * Sampling Action Companion Object
  * @author lawrence.daniels@gmail.com
  */
object SamplingAction {
  def apply(action: String, topic: String, partitions: js.Array[Int]) = {
    val samplingAction = makeNew[SamplingAction]
    samplingAction.action = action
    samplingAction.topic = topic
    samplingAction.partitions = partitions
    samplingAction
  }
}
