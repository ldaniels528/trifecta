package com.github.ldaniels528.trifecta.sjs.controllers

import com.github.ldaniels528.scalascript.Scope

import scala.scalajs.js

/**
  * Global Data Aware
  * @author lawrence.daniels@gmail.com
  */
@js.native
trait GlobalDataAware extends js.Object {
  self: Scope =>

  // properties
  var hideEmptyTopics: Boolean = js.native

  // functions
  var toggleHideShowEmptyTopics: js.Function0[Unit] = js.native

}
