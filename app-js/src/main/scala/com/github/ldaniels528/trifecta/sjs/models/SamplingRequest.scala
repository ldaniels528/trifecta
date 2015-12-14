package com.github.ldaniels528.trifecta.sjs.models

import com.github.ldaniels528.scalascript.util.ScalaJsHelper._

import scala.scalajs.js

/**
  * Represents a Sampling Request
  * @author lawrence.daniels@gmail.com
  */
@js.native
trait SamplingRequest extends js.Object {
  var topic: js.UndefOr[String] = js.native
  var partitionOffsets: js.Array[Int] = js.native
}

/**
  * Sampling Request Companion Object
  * @author lawrence.daniels@gmail.com
  */
object SamplingRequest {

  def apply(topic: String, partitionOffsets: js.Array[Int]) = {
    val request = makeNew[SamplingRequest]
    request.topic = topic
    request.partitionOffsets = partitionOffsets
    request
  }

}
