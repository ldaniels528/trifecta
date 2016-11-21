package org.scalajs.tjs.dom

import scala.scalajs.js
import scala.scalajs.js.annotation.ScalaJSDefined

/**
  * JS Blob
  * @author lawrence.daniels@gmail.com
  */
@js.native
class Blob(value: js.Any, options: BlobOptions = null) extends js.Object

/**
  * Blob Options
  * @author lawrence.daniels@gmail.com
  */
@ScalaJSDefined
class BlobOptions(val `type`: String = null) extends js.Object
