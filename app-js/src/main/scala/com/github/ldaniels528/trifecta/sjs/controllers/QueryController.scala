package com.github.ldaniels528.trifecta.sjs.controllers

import java.util.UUID

import com.github.ldaniels528.trifecta.sjs.controllers.GlobalLoading._
import com.github.ldaniels528.trifecta.sjs.controllers.QueryController._
import com.github.ldaniels528.trifecta.sjs.models.{TopicDetails, _}
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
import scala.scalajs.js.annotation.ScalaJSDefined
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
  //    Initialization Functions
  ///////////////////////////////////////////////////////////////////////////

  // restore the most recent query object
  $cookies.getObject[Query]("Query") foreach { query =>
    $scope.query = query
  }

  private def init() = {
    console.log("Initializing Query Controller...")
  }

  /**
    * Initialize the controller once the reference data has completed loading
    */
  $scope.onReferenceDataLoaded { _ => init() }

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
      name <- query.name
      queryString <- query.queryString
    } {
      executeQuery(query, name, queryString)
    }
  }

  /**
    * Executes the KQL query representing by the query string
    * (e.g. "select uid, pageid, uri, biData from birf_json_qa_picluster1 with json limit 150")
    */
  private def executeQuery(query: Query, name: String, queryString: String) = {
    // save this Query object as a cookie
    $cookies.putObject("Query", query)

    // setup the query clock
    query.running = true
    query.elapsedTime = 0.0
    query.startTime = js.Date.now()
    updateQueryClockEachSecond(query)

    // execute the query
    queryService.executeQuery(name, queryString).withGlobalLoading.withTimer("Executing query") onComplete {
      case Success(results) =>
        //console.log(s"results = ${angular.toJson(results)}")
        $scope.$apply { () =>
          query.running = false
          val savedResult = results.toSavedResult(queryString, query.computeRunTime)
          importResults(savedResult)

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

  $scope.importResults = (aResults: js.UndefOr[SavedResult]) => aResults foreach importResults

  private def importResults(savedResult: SavedResult) = {
    $scope.query.queryString = savedResult.queryString
    $scope.query.topic = savedResult.topic
    $scope.query.rows = savedResult.rows
    $scope.query.columns = savedResult.columns.map { columns =>
      js.Array("__partition", "__offset") ++ columns.filterNot(_.startsWith("__"))
    }
    $scope.query.runTimeMillis = savedResult.runTimeMillis
  }

  $scope.isSelected = (aTopic: js.UndefOr[TopicDetails], aPartition: js.UndefOr[PartitionDetails], aResults: js.UndefOr[QueryRow], anIndex: js.UndefOr[Int]) => {
    (for {
      topic <- aTopic.flatMap(_.topic)
      partition <- aPartition
      resultsTopic <- aResults.flatMap(_.topic)
      index <- anIndex
    } yield {
      topic == resultsTopic &&
        $scope.partitionAt(index) == partition.partition &&
        partition.offset.exists(o => $scope.offsetAt(index).exists(_ == o))
    }).isTrue
  }

  $scope.offsetAt = (anIndex: js.UndefOr[Int]) => {
    for {
      index <- anIndex
      results <- $scope.query.rows
      offset <- results(index).__offset
    } yield offset
  }

  $scope.partitionAt = (anIndex: js.UndefOr[Int]) => {
    for {
      index <- anIndex
      results <- $scope.query.rows
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

  $scope.saveQuery = (aQuery: js.UndefOr[Query]) => {
    for {
      query <- aQuery
      name <- query.name
      topic <- query.topic
      queryString <- query.queryString
    } {
      query.syncing = true
      $log.info(s"Uploading query '$name' (topic $topic)...")
      queryService.saveQuery(name, topic, queryString).withGlobalLoading.withTimer("Saving query") onComplete {
        case Success(response) =>
          $scope.$apply { () =>
            query.syncing = false
          }
          toaster.success(s"Query '$name' saved.")
        case Failure(e) =>
          $scope.$apply { () => query.syncing = false }
          errorPopup("Error saving query", e)
      }
    }
  }

  /**
    * Selects a query query from the list
    * @@param mySavedQuery the saved query object to select
    */
  $scope.selectQuery = (aQuery: js.UndefOr[Query]) => aQuery foreach { query =>
    $scope.query = query
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

}

/**
  * Query Controller Companion
  * @author lawrence.daniels@gmail.com
  */
object QueryController {
  var fileId: Int = 0

  @ScalaJSDefined
  class SavedResult(var uid: js.UndefOr[String],
                    var queryString: js.UndefOr[String],
                    var topic: js.UndefOr[String],
                    var columns: js.UndefOr[js.Array[String]],
                    var rows: js.UndefOr[js.Array[QueryRow]],
                    var runTimeMillis: js.UndefOr[Double]) extends QueryResultSet

  /**
    * Query Controller Scope
    * @author lawrence.daniels@gmail.com
    */
  @js.native
  trait QueryScope extends Scope
    with GlobalDataAware with GlobalLoading with GlobalErrorHandling
    with ReferenceDataAware {
    // properties
    var collection: js.UndefOr[js.Object] = js.native

    // functions
    var deleteQuery: js.Function1[js.UndefOr[Int], Unit] = js.native
    var displayLabel: js.Function1[js.UndefOr[String], js.UndefOr[String]] = js.native
    var downloadResults: js.Function1[js.UndefOr[SavedResult], Unit] = js.native
    var executeQuery: js.Function1[js.UndefOr[Query], Unit] = js.native
    var expandQueryTopic: js.Function1[js.UndefOr[TopicDetails], Unit] = js.native
    var importResults: js.Function1[js.UndefOr[SavedResult], Unit] = js.native
    var isSelected: js.Function4[js.UndefOr[TopicDetails], js.UndefOr[PartitionDetails], js.UndefOr[QueryRow], js.UndefOr[Int], Boolean] = js.native
    var offsetAt: js.Function1[js.UndefOr[Int], js.UndefOr[Int]] = js.native
    var partitionAt: js.Function1[js.UndefOr[Int], js.UndefOr[Int]] = js.native
    var removeResult: js.Function2[js.UndefOr[TopicDetails], js.UndefOr[SavedResult], Unit] = js.native
    var saveQuery: js.Function1[js.UndefOr[Query], Unit] = js.native
    var selectQuery: js.Function1[js.UndefOr[Query], Unit] = js.native
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