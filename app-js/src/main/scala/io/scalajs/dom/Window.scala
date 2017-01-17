package io.scalajs.dom

import Window.URL

import scala.scalajs.js

/**
  * JS Window
  * @author lawrence.daniels@gmail.com
  */
@js.native
trait Window extends js.Object {

  def URL: URL = js.native

}

/**
  * JS Window Companion
  * @author lawrence.daniels@gmail.com
  */
object Window {

  @js.native
  trait URL extends js.Object {

    def createObjectURL(value: js.Any): js.Any = js.native

  }

}
