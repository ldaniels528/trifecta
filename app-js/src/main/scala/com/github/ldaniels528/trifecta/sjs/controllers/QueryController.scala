package com.github.ldaniels528.trifecta.sjs.controllers

import com.github.ldaniels528.scalascript.core.TimerConversions._
import com.github.ldaniels528.scalascript.core._
import com.github.ldaniels528.scalascript.extensions.Toaster
import com.github.ldaniels528.scalascript.util.ScalaJsHelper._
import com.github.ldaniels528.scalascript.{Controller, Scope, angular, injected}
import com.github.ldaniels528.trifecta.sjs.controllers.GlobalLoading._
import com.github.ldaniels528.trifecta.sjs.controllers.ReferenceDataAware._
import com.github.ldaniels528.trifecta.sjs.models._
import com.github.ldaniels528.trifecta.sjs.services.QueryService
import com.github.ldaniels528.trifecta.sjs.util.NamingUtil
import org.scalajs.dom
import org.scalajs.dom.console

import scala.concurrent.duration._
import scala.scalajs.concurrent.JSExecutionContext.Implicits.runNow
import scala.scalajs.js
import scala.scalajs.js.JSConverters._
import scala.util.{Failure, Success}

/**
  * Query Controller
  * @author lawrence.daniels@gmail.com
  */
class QueryController($scope: QueryControllerScope, $log: Log, $timeout: Timeout, toaster: Toaster,
                      @injected("QuerySvc") querySvc: QueryService)
  extends Controller {

  implicit val scope = $scope

  $scope.collection = js.undefined
  $scope.savedQueries = emptyArray

  /**
    * Creates a new default saved query instance
    * @return {{running: boolean, results: null, mappings: null, ascending: boolean, sortField: null}}
    */
  $scope.savedQuery = newQuery

  ///////////////////////////////////////////////////////////////////////////
  //    Initialization Functions
  ///////////////////////////////////////////////////////////////////////////

  $scope.init = () => {
    console.log("Initializing Query Controller...")
    $scope.findNonEmptyTopic() foreach { myCollection =>
      $log.info(s"Setting first collection => ${angular.toJson(myCollection)}")
      $scope.selectQueryCollection(myCollection)
    }
  }

  ///////////////////////////////////////////////////////////////////////////
  //    Query Functions
  ///////////////////////////////////////////////////////////////////////////

  $scope.cancelNewQuery = (aSavedQuery: js.UndefOr[Query]) => aSavedQuery foreach { savedQuery =>
    if (savedQuery.newFile.contains(true)) {
      for {
        topicName <- savedQuery.topic
        topic <- $scope.findTopicByName(topicName)
      } {
        // remove the saved query from the list
        val savedQueries = topic.savedQueries.getOrElse(emptyArray)
        var index = savedQueries.indexOf(savedQuery)
        if (index != -1) {
          savedQueries.remove(index)
        }

        if ($scope.savedQuery == savedQuery) {
          $scope.savedQuery = newQuery
        }
      }
    }
  }

  /**
    * Removes a saved query from the list by index
    */
  $scope.deleteQuery = (anIndex: js.UndefOr[Int]) => anIndex foreach { index =>
    $scope.savedQueries.remove(index)
  }

  /**
    * Downloads the query results as CSV
    */
  $scope.downloadResults = (aResults: js.UndefOr[js.Array[QueryRow]]) => aResults foreach { results =>
    querySvc.transformResultsToCSV(results).withGlobalLoading.withTimer("Transforming results") onComplete {
      case Success(response) =>
        $log.info(s"response = ${angular.toJson(response)}")
      case Failure(e) =>
        $scope.addErrorMessage(e.displayMessage)
        toaster.error("CSV Download failed")
    }
  }

  /**
    * Executes the KQL query representing by the query string
    */
  $scope.executeQuery = (aSavedQuery: js.UndefOr[Query]) => aSavedQuery foreach { mySavedQuery =>
    mySavedQuery.running = true

    // setup the query clock
    mySavedQuery.queryElaspedTime = 0
    mySavedQuery.queryStartTime = new js.Date().getTime().toInt
    updatesQueryClock(mySavedQuery)

    for {
      name <- mySavedQuery.name
      topic <- mySavedQuery.topic
      queryString <- mySavedQuery.queryString
    } {
      // execute the query
      querySvc.executeQuery(name, topic, queryString).withGlobalLoading.withTimer("Executing query") onComplete {
        case Success(results) =>
          mySavedQuery.running = false
          val mappings = generateDataArray(results.labels, results.values)
          val mySavedResult = SavedResult(name = new js.Date().toTimeString(), results = results.values)

          // make sure the results array exists
          if (mySavedQuery.results.isEmpty) mySavedQuery.results = emptyArray[QueryRow]
          // TODO what's going on with this?
          //mySavedQuery.results.foreach(_.push(mySavedResult))
          mySavedQuery.savedResult = mySavedResult
        case Failure(e) =>
          mySavedQuery.running = false
          $scope.addErrorMessage(e.displayMessage)
      }
    }
  }

  $scope.expandQueryCollection = (aCollection: js.UndefOr[TopicDetails]) => aCollection foreach { collection =>
    collection.queriesExpanded = !collection.queriesExpanded.contains(true)
    if (collection.queriesExpanded.contains(true)) {
      loadQueriesByTopic(collection) onComplete {
        case Success(_) =>
        case Failure(e) =>
          $scope.addErrorMessage(e.displayMessage)
      }
    }
  }

  $scope.filterLabels = (aLabels: js.UndefOr[js.Array[String]]) => aLabels map { labels =>
    labels.init
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
    }).contains(true)
  }

  private def loadQueriesByTopic(topic: TopicDetails) = {
    topic.loading = true
    val outcome = querySvc.getQueriesByTopic(topic.topic).withGlobalLoading.withTimer("Loading queries by topic")
    outcome onComplete {
      case Success(queries) =>
        topic.loading = false
        topic.savedQueries = queries
        queries.foreach(_.collection = topic)
      case Failure(e) =>
        topic.loading = false
    }
    outcome
  }

  $scope.offsetAt = (anIndex: js.UndefOr[Int]) => {
    for {
      index <- anIndex
      results <- $scope.savedQuery.savedResult.flatMap(_.results)
      offset <- results(index).__offset
    } yield offset
  }

  $scope.partitionAt = (anIndex: js.UndefOr[Int]) => {
    for {
      index <- anIndex
      results <- $scope.savedQuery.savedResult.flatMap(_.results)
      partition <- results(index).__partition
    } yield partition
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
      querySvc.saveQuery(name, topic, queryString).withGlobalLoading.withTimer("Saving query") onComplete {
        case Success(response) =>
          query.modified = false
          query.newFile = false
          query.syncing = false
          toaster.success(s"Query '$name' saved.")
        case Failure(e) =>
          query.syncing = false
          $scope.addErrorMessage(e.displayMessage)
      }
    }
  }

  /**
    * Select the given collection
    * @@param collection the given collection
    */
  $scope.selectQueryCollection = (aCollection: js.UndefOr[TopicDetails]) => aCollection foreach { collection =>
    $scope.collection = collection

    // is the collection expanded?
    if (!collection.queriesExpanded.contains(true)) {
      $scope.expandQueryCollection(collection)
    }
    else {
      collection.savedQueries foreach { queries =>
        $scope.selectQuery(queries.headOption.orUndefined)
      }
    }
  }

  /**
    * Selects a query query from the list
    * @@param mySavedQuery the saved query object to select
    */
  $scope.selectQuery = (aSavedQuery: js.UndefOr[Query]) => aSavedQuery foreach { mySavedQuery =>
    $scope.savedQuery = mySavedQuery
    if (mySavedQuery.collection != $scope.collection) {
      $scope.collection = mySavedQuery.collection
    }
  }

  $scope.selectQueryResults = (aSavedQuery: js.UndefOr[Query], anIndex: js.UndefOr[Int]) => {
    for {
      mySavedQuery <- aSavedQuery
      index <- anIndex
    } {
      $scope.savedQuery = mySavedQuery
      //mySavedQuery.savedResult = mySavedQuery.results.map(_.apply(index))
    }
  }

  $scope.setUpNewQueryDocument = (aTopic: js.UndefOr[TopicDetails]) => aTopic foreach { topic =>
    if (topic.savedQueries.isEmpty) topic.savedQueries = emptyArray[Query]
    topic.savedQueries.foreach(_.push(newQueryScript(topic)))
    $scope.savedQuery = topic.savedQueries.map(_.head).orNull
  }

  $scope.toggleSortField = (aSavedQuery: js.UndefOr[Query], sortField: js.UndefOr[String]) => {
    aSavedQuery.flatMap(_.savedResult) foreach { mySavedResult =>
      mySavedResult.ascending = true
      mySavedResult.sortField = sortField
    }
  }

  private def generateDataArray(allLabels: js.Array[String], results: js.Array[QueryRow]) = {
    $scope.filterLabels(allLabels) map { myLabels =>
      QueryResultSet(labels = myLabels, values = results)
    }
  }

  /**
    * Indicates whether the given saved query (name) exists
    * @param topic the parent topic
    * @param name the saved query name
    * @return {boolean}
    */
  private def nameExists(topic: TopicDetails, name: String) = {
    topic.savedQueries.exists(_.exists(_.name.contains(name)))
  }

  /**
    * Returns a new query object
    * @return  a new [[Query query object]]
    */
  private def newQuery = Query(name = "UntitledName", topic = "default")

  /**
    * Constructs a new query script data object
    * @return {{name: string, topic: string, queryString: string, newFile: boolean, modified: boolean}}
    */
  private def newQueryScript(topic: TopicDetails) = {
    Query(name = NamingUtil.getUntitledName(topic)(nameExists), topic = topic.topic)
  }

  /**
    * Schedules the query clock to result until the query has completed.
    */
  private def updatesQueryClock(mySavedQuery: Query) {
    mySavedQuery.queryElaspedTime = mySavedQuery.queryStartTime.map(t => (new js.Date().getTime().toInt - t) / 1000)
    if (mySavedQuery.running.contains(true)) {
      $timeout(() => updatesQueryClock(mySavedQuery), 1.second)
    }
  }

  ///////////////////////////////////////////////////////////////////////////
  //    Event Handler Functions
  ///////////////////////////////////////////////////////////////////////////

  /**
    * Initialize the controller once the reference data has completed loading
    */
  $scope.$on(REFERENCE_DATA_LOADED, (event: dom.Event, data: ReferenceData) => $scope.init())

}

/**
  * Query Controller Scope
  * @author lawrence.daniels@gmail.com
  */
@js.native
trait QueryControllerScope extends Scope with GlobalDataAware with GlobalLoading with GlobalErrorHandling with ReferenceDataAware {
  // properties
  var collection: js.UndefOr[js.Object] = js.native
  var savedQuery: Query = js.native
  var savedQueries: js.Array[Query] = js.native

  // functions
  var init: js.Function0[Unit] = js.native
  var cancelNewQuery: js.Function1[js.UndefOr[Query], Unit] = js.native
  var deleteQuery: js.Function1[js.UndefOr[Int], Unit] = js.native
  var downloadResults: js.Function1[js.UndefOr[js.Array[QueryRow]], Unit] = js.native
  var executeQuery: js.Function1[js.UndefOr[Query], Unit] = js.native
  var filterLabels: js.Function1[js.UndefOr[js.Array[String]], js.UndefOr[js.Array[String]]] = js.native
  var expandQueryCollection: js.Function1[js.UndefOr[TopicDetails], Unit] = js.native
  var isSelected: js.Function4[js.UndefOr[TopicDetails], js.UndefOr[PartitionDetails], js.UndefOr[QueryRow], js.UndefOr[Int], Boolean] = js.native
  var offsetAt: js.Function1[js.UndefOr[Int], js.UndefOr[Int]] = js.native
  var partitionAt: js.Function1[js.UndefOr[Int], js.UndefOr[Int]] = js.native
  var saveQuery: js.Function1[js.UndefOr[Query], Unit] = js.native
  var selectQueryCollection: js.Function1[js.UndefOr[TopicDetails], Unit] = js.native
  var selectQuery: js.Function1[js.UndefOr[Query], Unit] = js.native
  var selectQueryResults: js.Function2[js.UndefOr[Query], js.UndefOr[Int], Unit] = js.native
  var setUpNewQueryDocument: js.Function1[js.UndefOr[TopicDetails], Unit] = js.native
  var toggleSortField: js.Function2[js.UndefOr[Query], js.UndefOr[String], Unit] = js.native

}