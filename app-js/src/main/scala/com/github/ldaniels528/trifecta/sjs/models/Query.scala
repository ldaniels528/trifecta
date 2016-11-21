package com.github.ldaniels528.trifecta.sjs.models

import org.scalajs.nodejs.util.ScalaJsHelper._

import scala.scalajs.js

/**
  * Represents a Query Model
  * @author lawrence.daniels@gmail.com
  */
@js.native
trait Query extends js.Object {
  var name: js.UndefOr[String] = js.native
  var queryString: js.UndefOr[String] = js.native

  // query results
  var topic: js.UndefOr[String] = js.native
  var columns: js.UndefOr[js.Array[String]] = js.native
  var rows: js.UndefOr[js.Array[QueryRow]] = js.native

  // ui-specific sorting
  var ascending: js.UndefOr[Boolean] = js.native
  var sortField: js.UndefOr[String] = js.native

  // ui-specific properties
  var elapsedTime: js.UndefOr[Double] = js.native
  var modified: js.UndefOr[Boolean] = js.native
  var startTime: js.UndefOr[Double] = js.native
  var runTimeMillis: js.UndefOr[Double] = js.native
  var running: js.UndefOr[Boolean] = js.native
  var syncing: js.UndefOr[Boolean] = js.native
}

/**
  * Query Companion Object
  * @author lawrence.daniels@gmail.com
  */
object Query {

  def apply(name: js.UndefOr[String]) = {
    val query = New[Query]
    query.name = name
    query
  }

  /**
    * Query Enrichment
    * @param query the given [[Query query]]
    */
  final implicit class QueryEnrichment(val query: Query) extends AnyVal {

    @inline
    def computeRunTime = query.startTime.map(t => (js.Date.now() - t) / 1000.0)

    @inline
    def computeElapsedRunTime = query.startTime.map(t => (js.Date.now() - t) / 1000.0)

  }

}