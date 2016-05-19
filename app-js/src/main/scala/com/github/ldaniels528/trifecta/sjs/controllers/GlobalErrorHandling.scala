package com.github.ldaniels528.trifecta.sjs.controllers

import com.github.ldaniels528.meansjs.angularjs.Scope
import com.github.ldaniels528.meansjs.util.ScalaJsHelper._

import scala.scalajs.js

/**
  * Created by ldaniels on 12/14/15.
  */
@js.native
trait GlobalErrorHandling extends js.Object {
  self: Scope =>

  // properties
  var globalMessages: js.Array[GlobalMessage] = js.native

  // functions
  var addErrorMessage: js.Function1[js.UndefOr[String], Unit] = js.native
  var addInfoMessage: js.Function1[js.UndefOr[String], Unit] = js.native
  var addWarningMessage: js.Function1[js.UndefOr[String], Unit] = js.native
  var removeAllMessages: js.Function0[Unit] = js.native
  var removeMessage: js.Function1[js.UndefOr[Int], Unit] = js.native

}

@js.native
trait GlobalMessage extends js.Object {
  var text: String = js.native
  var `type`: String = js.native
}

object GlobalMessage {
  def apply(text: String, `type`: String) = {
    val message = New[GlobalMessage]
    message.text = text
    message.`type` = `type`
    message
  }
}