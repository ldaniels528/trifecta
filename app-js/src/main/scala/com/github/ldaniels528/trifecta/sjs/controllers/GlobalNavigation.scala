package com.github.ldaniels528.trifecta.sjs.controllers

import io.scalajs.npm.angularjs.Scope

import scala.scalajs.js

/**
  * Global Navigation
  * @author lawrence.daniels@gmail.com
  */
@js.native
trait GlobalNavigation extends js.Object {
  self: Scope =>

  var switchToMessage: js.Function3[js.UndefOr[String], js.UndefOr[Int], js.UndefOr[Int], Unit] = js.native

}
