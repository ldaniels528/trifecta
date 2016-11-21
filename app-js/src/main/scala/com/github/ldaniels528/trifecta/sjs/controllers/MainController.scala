package com.github.ldaniels528.trifecta.sjs.controllers

import com.github.ldaniels528.trifecta.sjs.RootScope
import com.github.ldaniels528.trifecta.sjs.controllers.GlobalLoading._
import com.github.ldaniels528.trifecta.sjs.controllers.MainController._
import com.github.ldaniels528.trifecta.sjs.models._
import com.github.ldaniels528.trifecta.sjs.services.TopicService
import org.scalajs.angularjs.AngularJsHelper._
import org.scalajs.angularjs._
import org.scalajs.angularjs.toaster.Toaster
import org.scalajs.dom
import org.scalajs.dom.browser.console
import org.scalajs.nodejs.util.ScalaJsHelper._
import org.scalajs.sjs.JsUnderOrHelper._
import org.scalajs.sjs.PromiseHelper._

import scala.concurrent.duration._
import scala.language.postfixOps
import scala.scalajs.concurrent.JSExecutionContext.Implicits.queue
import scala.scalajs.js
import scala.scalajs.js.JSConverters._
import scala.util.{Failure, Success}

/**
  * Main Controller
  * @author lawrence.daniels@gmail.com
  */
case class MainController($scope: MainScope, $location: Location, $timeout: Timeout, toaster: Toaster,
                          @injected("TopicService") topicService: TopicService)
  extends Controller with PopupMessages {

  implicit val scope: Scope with GlobalLoading = $scope
  private var loading: Int = 0

  // reference data
  $scope.brokers = emptyArray
  $scope.consumers = emptyArray
  $scope.consumerGroupCache = js.Dictionary[js.Array[ConsumerGroup]]()
  $scope.messageBlob = MessageBlob(keyFormat = "UUID", keyAuto = true)
  $scope.replicas = emptyArray
  $scope.topics = emptyArray
  $scope.hideEmptyTopics = true

  // queries
  $scope.query = Query(name = "UntitledName")
  $scope.storedQueries = emptyArray

  // reference data flags
  $scope.referenceDataLoading = false
  $scope.brokersLoading = false
  $scope.consumersLoading = false
  $scope.topicsLoading = false

  ////////////////////////////////////////////////////////////////
  //    Main Tab Functions
  ///////////////////////////////////////////////////////////////

  $scope.tabs = js.Array(
    MainTab(
      name = "Inspect",
      contentURL = "/inspect",
      imageURL = "/assets/images/tabs/main/inspect-24.png"
    ), MainTab(
      name = "Observe",
      contentURL = "/observe",
      imageURL = "/assets/images/tabs/main/observe-24.png"
    ), MainTab(
      name = "Publish",
      contentURL = "/publish",
      imageURL = "/assets/images/tabs/main/publish-24.png"
    ), MainTab(
      name = "Query",
      contentURL = "/query",
      imageURL = "/assets/images/tabs/main/query-24.png"
    ), MainTab(
      name = "Decoders",
      contentURL = "/decoders",
      imageURL = "/assets/images/tabs/main/decoders-24.png"
    ))

  // select the current tab
  $scope.tab = determineActiveTab()
  $scope.tabs.foreach(t => t.active = t == $scope.tab)
  console.log(s"Current main tab is ${$scope.tab.name}")

  /**
    * Sets the active tab
    */
  $scope.changeTab = (aTab: js.UndefOr[MainTab], anEvent: js.UndefOr[dom.Event]) => {
    aTab.foreach(tab => {
      console.log(s"Setting tab to '${tab.name}' (${tab.contentURL})...")
      $location.path(tab.contentURL)
      $scope.tab = tab
      $scope.tabs.foreach(t => t.active = t == tab)
    })
    anEvent.foreach(_.preventDefault())
  }

  /**
    * Determines whether the given tab is the active tab
    */
  $scope.isActiveTab = (aTab: js.UndefOr[MainTab]) => aTab.contains($scope.tab)

  /**
    * Determines the current active tab
    */
  private def determineActiveTab() = {
    val uri = $location.path()
    console.log(s"Looking for tab for '$uri'...")
    val tab = $scope.tabs.find(_.contentURL == uri) getOrElse {
      console.warn(s"Path '$uri' not found; returning default...")
      $scope.tabs.head
    }
    $scope.tabs.foreach(t => t.active = t == tab)
    tab
  }

  ////////////////////////////////////////////////////////////////
  //    Global Loading Functions
  ///////////////////////////////////////////////////////////////

  $scope.isLoading = () => loading > 0

  $scope.loadingStart = () => {
    loading += 1
    $timeout(() => {}, 30.second) // TODO force the loading to end
  }

  $scope.loadingStop = (promise: CancellablePromise) => {
    $timeout(() => loading -= 1, 0.5.second)
    ()
  }

  ////////////////////////////////////////////////////////////////
  //    Global Navigation Functions
  ///////////////////////////////////////////////////////////////

  $scope.switchToMessage = (aTopic: js.UndefOr[String], aPartition: js.UndefOr[Int], anOffset: js.UndefOr[Int]) => {
    console.info(s"aTopic = ${aTopic.orNull}, aPartition = ${aPartition.orNull}, anOffset = ${anOffset.orNull}")
    for {
      topic <- aTopic
      partition <- aPartition
      offset <- anOffset
    } {
      // select the appropriate tab
      $scope.tabs.find(_.name == "Observe") foreach { tab =>
        $scope.tabs.foreach(t => t.active = t == tab)
      }
      $location.url(s"/observe?topic=$topic&partition=$partition&offset=$offset")
    }
  }

  /////////////////////////////////////////////////////////////////////////////////
  //        JSON-related Functions
  /////////////////////////////////////////////////////////////////////////////////

  /**
    * Formats a JSON object as a color-coded JSON expression
    * @@param objStr the JSON object
    * @@param tabWidth the number of tabs to use in formatting
    * @return a pretty formatted JSON string
    */
  $scope.toPrettyJSON = (anObject: js.UndefOr[String], aTabWidth: js.UndefOr[Int]) => anObject map { value =>
    angular.toJson(angular.fromJson(value), pretty = true)
  }

  /////////////////////////////////////////////////////////////////////////////////
  //        Error-related Functions
  /////////////////////////////////////////////////////////////////////////////////

  $scope.globalMessages = emptyArray

  $scope.addErrorMessage = (aMessageText: js.UndefOr[String]) => aMessageText foreach { messageText =>
    $scope.globalMessages.push(GlobalMessage(`type` = "error", text = messageText))
    scheduleRemoval()
  }

  $scope.addInfoMessage = (aMessageText: js.UndefOr[String]) => aMessageText foreach { messageText =>
    $scope.globalMessages.push(GlobalMessage(`type` = "info", text = messageText))
    scheduleRemoval()
  }

  $scope.addWarningMessage = (aMessageText: js.UndefOr[String]) => aMessageText foreach { messageText =>
    $scope.globalMessages.push(GlobalMessage(`type` = "warning", text = messageText))
    scheduleRemoval()
  }

  $scope.removeAllMessages = () => $scope.globalMessages = emptyArray

  $scope.removeMessage = (anIndex: js.UndefOr[Int]) => anIndex foreach ($scope.globalMessages.splice(_, 1))

  private def scheduleRemoval() = {
    val messages = $scope.globalMessages
    val message = messages.last

    $timeout(() => {
      val index = messages.indexOf(message)
      console.info("Removing message[" + index + "]...")
      if (index != -1) {
        $scope.removeMessage(index)
      }
    }, 10000 + $scope.globalMessages.length * 500)
  }

  /////////////////////////////////////////////////////////////////////////////////
  //        Consumer Functions
  /////////////////////////////////////////////////////////////////////////////////

  $scope.getConsumers = () => $scope.consumers

  $scope.getConsumersForTopic = (aTopic: js.UndefOr[String]) => aTopic map { topic =>
    $scope.consumerGroupCache.getOrElseUpdate(topic,
      js.Array($scope.consumers
        .filter(c => aTopic ?== c.topic)
        .groupBy(_.consumerId.orNull) map { case (consumerId, details) => ConsumerGroup(consumerId, details) } toSeq: _*))
  }

  $scope.getConsumersForIdAndTopic = (aConsumerId: js.UndefOr[String], aTopic: js.UndefOr[String]) => {
    for {
      consumerId <- aConsumerId
      topic <- aTopic
    } yield $scope.consumers.filter(c => c.consumerId.contains(consumerId) && c.topic.contains(topic))
  }

  /////////////////////////////////////////////////////////////////////////////////
  //        Topic Functions
  /////////////////////////////////////////////////////////////////////////////////

  /**
    * Attempts to find and return the first non-empty topic however, if none are found, it returns the
    * first topic in the array
    * @return the first non-empty topic
    */
  $scope.findNonEmptyTopic = () => $scope.topics.find(_.totalMessages > 0).orUndefined

  $scope.findTopicByName = (topicName: String) => $scope.topics.find(_.topic == topicName).orUndefined

  $scope.getTopicIcon = (aTopic: js.UndefOr[TopicDetails], isSelected: js.UndefOr[Boolean]) => {
    for {
      topic <- aTopic
      selected <- isSelected
    } yield {
      if (topic.totalMessages == 0) "/assets/images/common/topic_alert-16.png"
      else if (selected) "/assets/images/common/topic_selected-16.png"
      else "/assets/images/common/topic-16.png"
    }
  }

  $scope.getTopicIconSelection = (isSelected: js.UndefOr[Boolean]) => {
    if (isSelected.isTrue) "/assets/images/common/topic_selected-16.png" else "/assets/images/common/topic-16.png"
  }

  $scope.getTopicNames = () => $scope.getTopics().map(_.topic)

  $scope.getTopics = () => {
    if ($scope.hideEmptyTopics) $scope.topics.filter(_.totalMessages > 0) else $scope.topics
  }

  $scope.isSelectedTopic = (aTopic: js.UndefOr[TopicDetails]) => {
    aTopic.exists(t => $scope.topic.exists(_.topic == t.topic))
  }

  $scope.selectTopic = (aTopic: js.UndefOr[TopicDetails]) => aTopic foreach { topic =>
    $scope.topic = topic
  }

  /**
    * Toggles the empty topic hide/show flag
    */
  $scope.toggleHideShowEmptyTopics = () => $scope.hideEmptyTopics = !$scope.hideEmptyTopics

  ///////////////////////////////////////////////////////////////////////////
  //    Event Handler Functions
  ///////////////////////////////////////////////////////////////////////////

  private def updateConsumerDeltas(deltas: js.Array[ConsumerDelta]) = {
    console.log(s"Received consumer deltas => ${angular.toJson(deltas)}")
    deltas foreach updateConsumerDelta
  }

  private def updateConsumerDelta(delta: ConsumerDelta) = {
    $scope.consumers.find(c => c.consumerId == delta.consumerId && c.topic == delta.topic && c.partition == delta.partition) match {
      case Some(consumer) =>
        console.log(s"Updating consumer => ${angular.toJson(consumer)}")
        consumer.update(delta)

        // clear the delta after 5 seconds
        $timeout(() => {
          consumer.deltaC = js.undefined
          consumer.deltaT = js.undefined
        }, 5.seconds)
      case None =>
        console.log(s"Adding new consumer => ${angular.toJson(delta)}")
        $scope.consumers.push(Consumer(delta))
        $scope.consumerGroupCache.clear()
    }
  }

  private def updateTopicDeltas(deltas: js.Array[PartitionDelta]) = {
    for {
      delta <- deltas
      partitionId <- delta.partition
      topic <- $scope.topics.find(t => delta.topic.contains(t.topic)).orUndefined
    } {
      topic.replace(delta)

      // clear the delta after 5 seconds
      $timeout(() => topic(partitionId).foreach(_.delta = js.undefined), 5.seconds)
    }
  }

  /**
    * React to incoming consumer deltas
    */
  $scope.onConsumerDeltas { deltas => $scope.$apply(() => updateConsumerDeltas(deltas)) }

  /**
    * React to incoming topic deltas
    */
  $scope.onTopicDeltas { deltas => $scope.$apply(() => updateTopicDeltas(deltas)) }

  /////////////////////////////////////////////////////////////////////////////////
  //        Initialization
  /////////////////////////////////////////////////////////////////////////////////

  /**
    * Pre-load the reference data
    */
  $scope.init = () => {
    $scope.referenceDataLoading = true

    val promisedBrokers = loadBrokers()
    val promisedTopics = loadTopics()
    val promisedConsumers = loadConsumers()

    val outcome = for {
      brokers <- promisedBrokers
      topics <- promisedTopics
      consumers <- promisedConsumers
    } yield (brokers, topics, consumers)

    outcome onComplete {
      case Success((brokers, topics, consumers)) =>
        console.info(s"Reference data loaded: ${topics.length} topics, ${consumers.length} consumers, ${brokers.length} brokers")
        $scope.$apply(() => $scope.referenceDataLoading = false)

        // broadcast the events
        $scope.broadcastReferenceDataLoaded(ReferenceData(
          brokers = brokers,
          consumers = consumers,
          topics = $scope.topics,
          topic = $scope.topic
        ))
      case Failure(e) =>
        $scope.$apply(() => $scope.referenceDataLoading = false)
        toaster.error("Error loading topic, broker and consumer information")
        console.error(s"Error loading reference data: ${e.displayMessage}")
    }
  }

  private def loadBrokers() = {
    $scope.brokersLoading = true
    val promisedBrokers = topicService.getBrokerGroups.withGlobalLoading.withTimer("Retrieving brokers")
    promisedBrokers onComplete {
      case Success(brokers) =>
        console.info(s"Loaded ${brokers.length} broker(s)")
        $scope.$apply { () =>
          $scope.brokersLoading = false
          $scope.brokers = brokers
        }
      case Failure(e) =>
        $scope.$apply(() => $scope.brokersLoading = false)
        toaster.error("Error loading Kafka brokers")
        console.error(s"Error loading Kafka brokers: ${e.displayMessage}")
    }
    promisedBrokers
  }

  private def loadConsumers() = {
    $scope.consumersLoading = true
    val promisedConsumers = topicService.getConsumers.withGlobalLoading.withTimer("Retrieving consumers")
    promisedConsumers onComplete {
      case Success(consumers) =>
        console.info(s"Loaded ${consumers.length} consumer(s)")
        $scope.$apply { () =>
          $scope.consumersLoading = false
          $scope.consumers = consumers
          $scope.consumerGroupCache.clear()
        }
        $scope.broadcastConsumersLoaded(consumers)
      case Failure(e) =>
        $scope.$apply(() => $scope.consumersLoading = false)
        toaster.error("Error loading consumer groups")
        console.error(s"Error loading consumer groups: ${e.displayMessage}")
    }
    promisedConsumers
  }

  private def loadTopics() = {
    $scope.topicsLoading = true
    val promisedTopics = topicService.getDetailedTopics.withGlobalLoading.withTimer("Retrieving topics")
    promisedTopics onComplete {
      case Success(topics) =>
        console.info(s"Loaded ${topics.length} topic(s)")
        val sortedTopics = enrichTopics(topics.sortBy(_.topic))
        $scope.$apply { () =>
          $scope.topicsLoading = false
          $scope.topic = sortedTopics.find(_.totalMessages > 0).orUndefined
          $scope.topics = sortedTopics
        }
        $scope.broadcastTopicsLoaded(topics)
      case Failure(e) =>
        $scope.$apply(() => $scope.topicsLoading = false)
        toaster.error("Error loading Kafka topics")
        console.error(s"Error loading Kafka topics: ${e.displayMessage}")
    }
    promisedTopics
  }

  private def enrichTopics(topics: js.Array[TopicDetails]) = {
    // set the default offset pointers for all topics
    for {
      t <- topics
      p <- t.partitions
    } p.offset = p.startOffset
    topics
  }

}

/**
  * Main Controller Companion
  * @author lawrence.daniels@gmail.com
  */
object MainController {

  /**
    * Main Controller
    * @author lawrence.daniels@gmail.com
    */
  @js.native
  trait MainScope extends RootScope
    with GlobalDataAware with GlobalLoading with GlobalErrorHandling with GlobalNavigation
    with MainTabManagement with ReferenceDataAware {
    // functions
    var init: js.Function0[Unit] = js.native
    var getDateFormat: js.Function1[js.UndefOr[Int], js.UndefOr[String]] = js.native
    var isActiveTab: js.Function1[js.UndefOr[MainTab], Boolean] = js.native
    var toPrettyJSON: js.Function2[js.UndefOr[String], js.UndefOr[Int], js.UndefOr[String]] = js.native

  }

}