package com.github.ldaniels528.trifecta.sjs.models

import com.github.ldaniels528.scalascript.util.ScalaJsHelper._
import scala.scalajs.js

/**
  * Query Collection
  * @author lawrence.daniels@gmail.com
  */
@js.native
trait QueryCollection extends js.Object  {
  var queriesExpanded: js.UndefOr[Boolean] = js.native
}

/**
  * Query Collection Companion Object
  * @author lawrence.daniels@gmail.com
  */
object QueryCollection {

  def apply() = {
    val qc = makeNew[QueryCollection]
    qc
  }

}