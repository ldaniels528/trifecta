package com.github.ldaniels528.trifecta.sjs.controllers

import io.scalajs.npm.angularjs.AngularJsHelper._
import io.scalajs.npm.angularjs.Controller
import io.scalajs.npm.angularjs.toaster.Toaster
import io.scalajs.dom.html.browser.console

/**
  * Popup Messages
  * @author lawrence.daniels@gmail.com
  */
trait PopupMessages {
  self: Controller =>

  def toaster: Toaster

  def errorPopup(message: String, e: Throwable = null) {
    toaster.error(message)
    console.error(s"$message ${Option(e).map(_.displayMessage) getOrElse ""}")
  }

  def warningPopup(message: String, e: Throwable = null) {
    toaster.warning(message)
    console.warn(s"$message ${Option(e).map(_.displayMessage) getOrElse ""}")
  }

}
