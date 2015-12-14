package com.github.ldaniels528.trifecta.sjs.models

import com.github.ldaniels528.scalascript.util.ScalaJsHelper._

import scala.scalajs.js

/**
  * Represents a Zookeeper Item
  * @author lawrence.daniels@gmail.com
  */
@js.native
trait ZkItem extends js.Object {
  var name: String = js.native
  var path: String = js.native
  var children: js.Array[ZkItem] = js.native
  var data: js.UndefOr[ZkData] = js.native
  var expanded: Boolean = js.native

  // ui-specific properties
  var loading: js.UndefOr[Boolean] = js.native
}

/**
  * Zookeeper Item
  * @author lawrence.daniels@gmail.com
  */
object ZkItem {

  def apply(name: String, path: String, expanded: Boolean) = {
    val item = makeNew[ZkItem]
    item.name = name
    item.path = path
    item.children = emptyArray
    item.expanded = expanded
    item
  }

}
