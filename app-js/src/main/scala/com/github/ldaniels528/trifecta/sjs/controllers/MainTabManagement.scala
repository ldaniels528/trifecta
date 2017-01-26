package com.github.ldaniels528.trifecta.sjs.controllers

import io.scalajs.npm.angularjs.Scope
import com.github.ldaniels528.trifecta.sjs.models.MainTab
import io.scalajs.dom

import scala.scalajs.js

/**
  * Main Tab Management
  * @author lawrence.daniels@gmail.com
  */
@js.native
trait MainTabManagement extends js.Object {
  self: Scope =>

  // properties
  var tab: MainTab = js.native
  var tabs: js.Array[MainTab] = js.native

  // functions
  var changeTab: js.Function2[js.UndefOr[MainTab], js.UndefOr[dom.Event], Unit] = js.native
}

