package com.github.ldaniels528.trifecta.sjs.models

import scala.scalajs.js
import scala.scalajs.js.UndefOr
import scala.scalajs.js.annotation.ScalaJSDefined

/**
  * Represents a Query Model
  * @author lawrence.daniels@gmail.com
  */
@ScalaJSDefined
class Query(var queryString: js.UndefOr[String] = js.undefined) extends js.Object {
  // query results
  var topic: js.UndefOr[String] = js.undefined
  var columns: js.UndefOr[js.Array[String]] = js.undefined
  var rows: js.UndefOr[js.Array[QueryRow]] = js.undefined

  // ui-specific sorting
  var ascending: js.UndefOr[Boolean] = js.undefined
  var sortField: js.UndefOr[String] = js.undefined

  // ui-specific properties
  var elapsedTime: js.UndefOr[Double] = js.undefined
  var modified: js.UndefOr[Boolean] = js.undefined
  var startTime: js.UndefOr[Double] = js.undefined
  var runTimeMillis: js.UndefOr[Double] = js.undefined
  var running: js.UndefOr[Boolean] = js.undefined
  var syncing: js.UndefOr[Boolean] = js.undefined
}

/**
  * Query Companion Object
  * @author lawrence.daniels@gmail.com
  */
object Query {

  /**
    * Query Result Set
    * @author lawrence.daniels@gmail.com
    */
  @ScalaJSDefined
  trait QueryResultSet extends js.Object {
    var topic: js.UndefOr[String]
    var columns: js.UndefOr[js.Array[String]]
    var rows: js.UndefOr[js.Array[QueryRow]]
  }

  @ScalaJSDefined
  class SavedResult(var uid: js.UndefOr[String],
                    var queryString: js.UndefOr[String],
                    var topic: js.UndefOr[String],
                    var columns: js.UndefOr[js.Array[String]],
                    var rows: js.UndefOr[js.Array[QueryRow]],
                    var runTimeMillis: js.UndefOr[Double]) extends QueryResultSet

  /**
    * Query Enrichment
    * @param query the given [[Query query]]
    */
  final implicit class QueryEnrichment(val query: Query) extends AnyVal {

    @inline
    def computeRunTime: UndefOr[Double] = query.startTime.map(t => (js.Date.now() - t) / 1000.0)

    @inline
    def computeElapsedRunTime: UndefOr[Double] = query.startTime.map(t => (js.Date.now() - t) / 1000.0)

  }

}