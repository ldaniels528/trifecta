package io.scalajs

import scala.scalajs.js
import scala.scalajs.js.annotation.JSName

/**
  * dom package object
  * @author lawrence.daniels@gmail.com
  */
package object dom {

  @js.native
  @JSName("window")
  object window extends Window

}
