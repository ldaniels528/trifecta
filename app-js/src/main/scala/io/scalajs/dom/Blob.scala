package io.scalajs.dom

import scala.scalajs.js
import scala.scalajs.js.annotation.ScalaJSDefined

/**
  * JS Blob
  * @see https://developer.mozilla.org/en-US/docs/Web/API/Blob
  * @author lawrence.daniels@gmail.com
  */
@js.native
class Blob(blobParts: js.Array[_], options: BlobPropertyBag = js.native) extends js.Object {

  def size: Int = js.native

  def `type`: String = js.native

  def slice(start: Int = js.native, end: Int = js.native, contentType: String = js.native): Blob = js.native

}

/**
  * Blob Property Bag
  * @author lawrence.daniels@gmail.com
  */
@ScalaJSDefined
class BlobPropertyBag(val `type`: String = null) extends js.Object
