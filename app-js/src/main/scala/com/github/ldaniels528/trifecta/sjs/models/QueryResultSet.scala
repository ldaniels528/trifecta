package com.github.ldaniels528.trifecta.sjs.models

import com.github.ldaniels528.scalascript.util.ScalaJsHelper._

import scala.scalajs.js

/**
  * Query Result Set
  * @author lawrence.daniels@gmail.com
  */
@js.native
trait QueryResultSet extends js.Object {
  var labels: js.Array[String] = js.native
  var values: js.Array[QueryRow] = js.native
}

/**
  * Query Result Set
  * @author lawrence.daniels@gmail.com
  */
object QueryResultSet {

  def apply(labels: js.Array[String], values: js.Array[QueryRow]) = {
    val rs = makeNew[QueryResultSet]
    rs.labels = labels
    rs.values = values
    rs
  }

}