package com.github.ldaniels528.trifecta.sjs.controllers

import java.util.UUID

import com.github.ldaniels528.trifecta.sjs.controllers.GlobalLoading._
import com.github.ldaniels528.trifecta.sjs.controllers.QueryController._
import com.github.ldaniels528.trifecta.sjs.models.Query._
import com.github.ldaniels528.trifecta.sjs.models.QueryRow._
import com.github.ldaniels528.trifecta.sjs.models.{PartitionDetails, Query, QueryRow, TopicDetails}
import com.github.ldaniels528.trifecta.sjs.services.QueryService
import org.scalajs.angularjs._
import org.scalajs.angularjs.cookies.Cookies
import org.scalajs.angularjs.toaster.Toaster
import org.scalajs.dom.browser.console
import org.scalajs.nodejs.util.ScalaJsHelper._
import org.scalajs.sjs.JsUnderOrHelper._
import org.scalajs.sjs.PromiseHelper._
import org.scalajs.tjs.dom.{Blob, BlobOptions, Element, window}

import scala.concurrent.duration._
import scala.scalajs.concurrent.JSExecutionContext.Implicits.queue
import scala.scalajs.js
import scala.util.{Failure, Success}

/**
  * Query Controller
  * @author lawrence.daniels@gmail.com
  */
case class QueryController($scope: QueryScope, $cookies: Cookies, $log: Log, $timeout: Timeout, toaster: Toaster,
                           @injected("QueryService") queryService: QueryService)
  extends Controller with PopupMessages {

  implicit val scope: Scope with GlobalLoading = $scope

  ///////////////////////////////////////////////////////////////////////////
  //    Query Functions
  ///////////////////////////////////////////////////////////////////////////

  /**
    * Removes a saved query from the list by index
    */
  $scope.deleteQuery = (anIndex: js.UndefOr[Int]) => anIndex foreach { index =>
    $scope.storedQueries.remove(index)
  }

  $scope.downloadResults = (aSavedResult: js.UndefOr[SavedResult]) => aSavedResult foreach downloadResults

  /**
    * Downloads the query results as CSV
    */
  private def downloadResults(savedResult: SavedResult) {
    fileId += 1
    val blob = new Blob(js.Array(angular.toJson(savedResult, pretty = true)), new BlobOptions(`type` = "application/json;charset=utf-8;"))
    val downloadLink = angular.dynamic.element("<a></a>").asInstanceOf[Element]
    downloadLink.attr("href", window.URL.createObjectURL(blob))
    downloadLink.attr("download", s"results-$fileId.json")
    downloadLink(0).click()
  }

  $scope.executeQuery = (aQuery: js.UndefOr[Query]) => {
    for {
      query <- aQuery
      queryString <- query.queryString
    } {
      executeQuery(query, queryString)
    }
  }

  /**
    * Executes the KQL query representing by the query string
    * (e.g. "select uid, pageid, uri, biData from birf_json_qa_picluster1 with json limit 150")
    */
  private def executeQuery(query: Query, queryString: String) = {
    // save this Query object as a cookie
    $cookies.putObject("Query", query)

    // setup the query clock
    query.running = true
    query.elapsedTime = 0.0
    query.startTime = js.Date.now()
    updateQueryClockEachSecond(query)

    // execute the query
    queryService.executeQuery(queryString).withGlobalLoading.withTimer("Executing query") onComplete {
      case Success(results) =>
        //console.log(s"results = ${angular.toJson(results, pretty = true)}")
        $scope.$apply { () =>
          query.running = false
          val savedResult = results.toSavedResult(queryString, query.computeRunTime)
          importResults(query, savedResult)

          // add the query results to the topic
          $scope.topics.find(t => results.topic.contains(t.topic)) foreach { topic =>
            topic += savedResult
            $scope.selectTopic(topic)
          }
        }
      case Failure(e) =>
        $scope.$apply { () =>
          query.running = false
          query.topic = js.undefined
          query.columns = js.undefined
          query.rows = js.undefined
        }
        errorPopup("Query failed", e)
    }
  }

  $scope.expandQueryTopic = (aTopic: js.UndefOr[TopicDetails]) => aTopic foreach { details =>
    details.queriesExpanded = !details.queriesExpanded.isTrue
  }

  $scope.displayLabel = (aLabel: js.UndefOr[String]) => aLabel map {
    case s if s.startsWith("__") => s.drop(2)
    case s => s
  }

  $scope.importResults = (aQuery: js.UndefOr[Query], aResults: js.UndefOr[SavedResult]) => {
    for {
      query <- aQuery
      results <- aResults
    } {
      importResults(query, results)
    }
  }

  private def importResults(query: Query, savedResult: SavedResult) = {
    query.queryString = savedResult.queryString
    query.topic = savedResult.topic
    query.rows = savedResult.rows
    query.runTimeMillis = savedResult.runTimeMillis

    // handle special columns (e.g. "__partition", "__offset" and "__error")
    val hasErrors = savedResult.rows.exists(_.exists(_.__error.isAssigned))
    val specialColumns = if(hasErrors) ColumnsWithError else ColumnsWithoutError
    query.columns = savedResult.columns.map { columns =>
      specialColumns ++ columns.filterNot(_.startsWith("__"))
    }
  }

  $scope.isSelected = (aTopic: js.UndefOr[TopicDetails], aPartition: js.UndefOr[PartitionDetails], aQuery: js.UndefOr[Query], aRow: js.UndefOr[QueryRow], anIndex: js.UndefOr[Int]) => {
    (for {
      topic <- aTopic.flatMap(_.topic)
      partition <- aPartition
      query <- aQuery
      results <- aRow
      resultsTopic <- results.topic
      index <- anIndex
    } yield {
      topic == resultsTopic &&
        $scope.partitionAt(query, index) == partition.partition &&
        partition.offset.exists(o => $scope.offsetAt(query, index).exists(_ == o))
    }).isTrue
  }

  $scope.offsetAt = (aQuery: js.UndefOr[Query], anIndex: js.UndefOr[Int]) => {
    for {
      query <- aQuery
      index <- anIndex
      results <- query.rows
      offset <- results(index).__offset
    } yield offset
  }

  $scope.partitionAt = (aQuery: js.UndefOr[Query], anIndex: js.UndefOr[Int]) => {
    for {
      query <- aQuery
      index <- anIndex
      results <- query.rows
      partition <- results(index).__partition
    } yield partition
  }

  $scope.removeResult = (aTopic: js.UndefOr[TopicDetails], aResult: js.UndefOr[SavedResult]) => {
    for {
      topic <- aTopic
      result <- aResult
    } {
      removeResult(topic, result)
    }
  }

  private def removeResult(topic: TopicDetails, result: SavedResult) = {
    for {
      results <- topic.queryResults
      index = results.indexWhere(_.uid ?== result.uid)
    } {
      results.remove(index)
    }
  }

  $scope.selectQueryTopic = (aTopic: js.UndefOr[TopicDetails]) => {
    $scope.selectTopic(aTopic)
    aTopic foreach { topic =>
      if (topic.query.nonAssigned) {
        topic.query = new Query(s"select * from ${topic.topic} with default")
      }
    }
    $scope.query = aTopic.flatMap(_.query)
  }

  $scope.toggleSortField = (aQuery: js.UndefOr[Query], sortField: js.UndefOr[String]) => aQuery foreach { query =>
    query.ascending = !query.ascending.isTrue
    query.sortField = sortField
  }

  /**
    * Schedules the query clock to result until the query has completed.
    */
  private def updateQueryClockEachSecond(query: Query) {
    query.elapsedTime = query.computeElapsedRunTime
    if (query.running.isTrue) {
      $timeout(() => updateQueryClockEachSecond(query), 1.second)
    }
  }

  ///////////////////////////////////////////////////////////////////////////
  //    Initialization Functions
  ///////////////////////////////////////////////////////////////////////////

  // restore the most recent query object
  $scope.selectQueryTopic($scope.topic)

  private def init(topics: js.Array[TopicDetails]) = {
    console.log("Initializing Query Controller...")
    $scope.$apply(() => $scope.selectQueryTopic($scope.topic))
  }

  /**
    * Initialize the controller once the reference data has completed loading
    */
  $scope.onTopicsLoaded { topics => init(topics) }

}

/**
  * Query Controller Companion
  * @author lawrence.daniels@gmail.com
  */
object QueryController {
  var fileId: Int = 0

  /**
    * Query Controller Scope
    * @author lawrence.daniels@gmail.com
    */
  @js.native
  trait QueryScope extends Scope
    with GlobalDataAware with GlobalLoading with GlobalErrorHandling
    with ReferenceDataAware {

    // properties
    var query: js.UndefOr[Query] = js.native

    // functions
    var deleteQuery: js.Function1[js.UndefOr[Int], Unit] = js.native
    var displayLabel: js.Function1[js.UndefOr[String], js.UndefOr[String]] = js.native
    var downloadResults: js.Function1[js.UndefOr[SavedResult], Unit] = js.native
    var executeQuery: js.Function1[js.UndefOr[Query], Unit] = js.native
    var expandQueryTopic: js.Function1[js.UndefOr[TopicDetails], Unit] = js.native
    var importResults: js.Function2[js.UndefOr[Query], js.UndefOr[SavedResult], Unit] = js.native
    var isSelected: js.Function5[js.UndefOr[TopicDetails], js.UndefOr[PartitionDetails], js.UndefOr[Query], js.UndefOr[QueryRow], js.UndefOr[Int], Boolean] = js.native
    var offsetAt: js.Function2[js.UndefOr[Query], js.UndefOr[Int], js.UndefOr[Int]] = js.native
    var partitionAt: js.Function2[js.UndefOr[Query], js.UndefOr[Int], js.UndefOr[Int]] = js.native
    var removeResult: js.Function2[js.UndefOr[TopicDetails], js.UndefOr[SavedResult], Unit] = js.native
    var selectQueryTopic: js.Function1[js.UndefOr[TopicDetails], Unit] = js.native
    var toggleSortField: js.Function2[js.UndefOr[Query], js.UndefOr[String], Unit] = js.native
  }

  /**
    * Saved Result Query Extensions
    * @param results the given [[QueryResultSet query result set]]
    */
  final implicit class SavedResultQueryExtensions(val results: QueryResultSet) extends AnyVal {

    def toSavedResult(queryString: String, runTimeMillis: js.UndefOr[Double]): SavedResult = {
      new SavedResult(
        uid = UUID.randomUUID().toString,
        queryString = queryString,
        topic = results.topic,
        columns = results.columns,
        rows = results.rows,
        runTimeMillis = runTimeMillis
      )
    }
  }

  /**
    * Topic Details Query Extensions
    * @param details the given [[TopicDetails topic details]]
    */
  final implicit class TopicDetailsQueryExtensions(val details: TopicDetails) extends AnyVal {

    def +=(savedResult: SavedResult) {
      // attach the results
      if (details.queryResults.isEmpty)
        details.queryResults = js.Array(savedResult)
      else
        details.queryResults.foreach(_.push(savedResult))

      // auto-expand the topic
      details.queriesExpanded = true
    }
  }

}