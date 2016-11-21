package org.scalajs.tjs.dom

import scala.scalajs.js
import scala.scalajs.js.annotation.JSBracketAccess

/**
  * Represents an Element
  * @author lawrence.daniels@gmail.com
  */
@js.native
trait Element extends js.Object {

  @JSBracketAccess
  def apply(index: Int): Element = js.native

  def attr(name: String, value: js.Any): Unit = js.native

  def click(): js.Any = js.native

}
