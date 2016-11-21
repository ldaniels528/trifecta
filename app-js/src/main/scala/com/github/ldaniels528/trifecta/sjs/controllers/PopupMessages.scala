package com.github.ldaniels528.trifecta.sjs.controllers

import org.scalajs.angularjs.AngularJsHelper._
import org.scalajs.angularjs.Controller
import org.scalajs.angularjs.toaster.Toaster
import org.scalajs.dom.browser.console

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
