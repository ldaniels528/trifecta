package com.github.ldaniels528.trifecta.sjs.models

import com.github.ldaniels528.scalascript.util.ScalaJsHelper._

import scala.scalajs.js

/**
  * Represents a Message Sampling Status
  * @author lawrence.daniels@gmail.com
  */
@js.native
trait SamplingStatus extends js.Object {
  var sessionId: js.UndefOr[String] = js.native
  var status: js.UndefOr[String] = js.native
}

/**
  * Sampling Status Companion Object
  * @author lawrence.daniels@gmail.com
  */
object SamplingStatus {
  val SAMPLING_STATUS_STARTED = "started"
  val SAMPLING_STATUS_STOPPED = "stopped"

  def apply(status: String) = {
    val samplingStatus = makeNew[SamplingStatus]
    samplingStatus.status = status
    samplingStatus
  }

}