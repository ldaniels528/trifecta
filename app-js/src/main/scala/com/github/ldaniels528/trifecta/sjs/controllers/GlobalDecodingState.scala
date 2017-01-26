package com.github.ldaniels528.trifecta.sjs.controllers

import io.scalajs.npm.angularjs.Scope

import scala.scalajs.js

/**
  * Global Decoding State
  * @author lawrence.daniels@gmail.com
  */
@js.native
trait GlobalDecodingState extends js.Object {
  self: Scope =>

  // properties
  var topicDecoding: js.Dictionary[Boolean] = js.native

  // CODEC functions
  var getDecodingState: js.Function1[js.UndefOr[String], js.UndefOr[Boolean]] = js.native
  var toggleDecodingState: js.Function1[js.UndefOr[String], Unit] = js.native
  var toPrettyJSON: js.Function2[js.UndefOr[String], js.UndefOr[Int], js.UndefOr[String]] = js.native

}
