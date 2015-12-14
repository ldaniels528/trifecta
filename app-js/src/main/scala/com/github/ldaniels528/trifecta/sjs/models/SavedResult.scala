package com.github.ldaniels528.trifecta.sjs.models

import com.github.ldaniels528.scalascript.util.ScalaJsHelper._

import scala.scalajs.js

/**
  * Saved Result
  * @author lawrence.daniels@gmail.com
  */
@js.native
trait SavedResult extends js.Object {
  var name: js.UndefOr[String] = js.native
  var results: js.UndefOr[js.Array[QueryRow]] = js.native
  var mappings: js.UndefOr[js.Dictionary[String]] = js.native
  var ascending: js.UndefOr[Boolean] = js.native
  var sortField: js.UndefOr[String] = js.native
}

/**
  * Saved Result Companion Object
  * @author lawrence.daniels@gmail.com
  */
object SavedResult {

  def apply(name: js.UndefOr[String],
            results: js.UndefOr[js.Array[QueryRow]] = js.undefined,
            mappings: js.UndefOr[js.Dictionary[String]] = js.undefined,
            ascending: js.UndefOr[Boolean] = false,
            sortField: js.UndefOr[String] = js.undefined) = {
    val result = makeNew[SavedResult]
    result.name = name
    result.results = results
    result.mappings = mappings
    result.ascending = ascending
    result.sortField = sortField
    result
  }

}