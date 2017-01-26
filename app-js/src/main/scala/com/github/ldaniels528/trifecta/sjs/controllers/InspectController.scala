package com.github.ldaniels528.trifecta.sjs.controllers

import com.github.ldaniels528.trifecta.sjs.controllers.GlobalLoading._
import com.github.ldaniels528.trifecta.sjs.controllers.InspectController._
import com.github.ldaniels528.trifecta.sjs.models._
import com.github.ldaniels528.trifecta.sjs.services.{BrokerService, ConsumerGroupService, TopicService, ZookeeperService}
import io.scalajs.npm.angularjs._
import io.scalajs.npm.angularjs.toaster.Toaster
import io.scalajs.dom
import io.scalajs.dom.html.browser.console
import io.scalajs.util.ScalaJsHelper._
import io.scalajs.util.JsUnderOrHelper._
import io.scalajs.util.PromiseHelper._

import scala.concurrent.duration._
import scala.scalajs.concurrent.JSExecutionContext.Implicits.queue
import scala.scalajs.js
import scala.scalajs.js.JSConverters._
import scala.scalajs.js.annotation.ScalaJSDefined
import scala.util.{Failure, Success}

/**
  * Inspect Controller
  * @author lawrence.daniels@gmail.com
  */
case class InspectController($scope: InspectScope, $location: Location, $log: Log, $routeParams: InspectRouteParams,
                             $timeout: Timeout, $interval: Interval, toaster: Toaster,
                             @injected("BrokerService") brokerService: BrokerService,
                             @injected("ConsumerGroupService") consumerGroupService: ConsumerGroupService,
                             @injected("TopicService") topicService: TopicService,
                             @injected("ZookeeperService") zookeeperService: ZookeeperService)
  extends Controller with PopupMessages {

  implicit val scope: Scope with GlobalLoading = $scope

  $scope.versions = js.Dictionary("0" -> "0.7.x", "1" -> "0.8.x", "2" -> "0.9.0.x", "3" -> "0.10.0.x", "4" -> "0.10.1.x")
  $scope.formats = js.Array("auto", "binary", "json", "plain-text")
  $scope.selected = FormatSelection(format = $scope.formats.head)
  $scope.zkItem = js.undefined
  $scope.zkItems = js.Array(ZkItem(name = "/ (root)", path = "/", expanded = false))

  ///////////////////////////////////////////////////////////////////////////
  //    Inspect Tab Functions
  ///////////////////////////////////////////////////////////////////////////

  $scope.inspectTabs = js.Array(
    MainTab(name = "Brokers", contentURL = "/inspect?mode=brokers", imageURL = "/assets/images/tabs/inspect/brokers.png"),
    MainTab(name = "Consumers", contentURL = "/inspect?mode=consumers", imageURL = "/assets/images/tabs/inspect/consumers.png"),
    MainTab(name = "Leaders", contentURL = "/inspect?mode=leaders", imageURL = "/assets/images/tabs/inspect/topics.png"),
    MainTab(name = "Replicas", contentURL = "/inspect?mode=replicas", imageURL = "/assets/images/tabs/inspect/replicas-24.png"),
    MainTab(name = "Zookeeper", contentURL = "/inspect?mode=zookeeper", imageURL = "/assets/images/tabs/inspect/zookeeper.png"))

  // select the default tab and make it active
  $scope.inspectTab = determineActiveTab()
  $scope.inspectTabs.foreach(t => t.active = t == $scope.inspectTab)
  console.log(s"Current inspect tab is ${$scope.inspectTab.name}")

  /**
    * Sets the active tab
    */
  $scope.changeInspectTab = (aTab: js.UndefOr[MainTab], anEvent: js.UndefOr[dom.Event]) => {
    aTab.foreach(tab => {
      console.log(s"Setting Inspect tab to '${tab.name}' (${tab.contentURL})...")
      $location.search("mode", tab.name.toLowerCase)
      $scope.inspectTab = tab
      $scope.inspectTabs.foreach(t => t.active = t == tab)
    })
    anEvent.foreach(_.preventDefault())
  }

  /**
    * Determines whether the given tab is the active tab
    */
  $scope.isActiveInspectTab = (aTab: js.UndefOr[MainTab]) => aTab.contains($scope.inspectTab)

  /**
    * Determines the current active tab
    */
  private def determineActiveTab() = {
    console.log(s"Inspect: mode = ${$routeParams.mode}")
    $routeParams.mode.toOption match {
      case Some(mode) =>
        $scope.inspectTabs.find(_.name.toLowerCase == mode) getOrElse $scope.inspectTabs.head
      case None =>
        val uri = $location.path()
        $scope.inspectTabs.find(_.contentURL == uri) getOrElse {
          console.warn(s"Path '$uri' not found; returning default...")
          $scope.inspectTabs.head
        }
    }
  }

  ///////////////////////////////////////////////////////////////////////////
  //    Initialization Functions
  ///////////////////////////////////////////////////////////////////////////

  $scope.init = () => {
    console.log("Initializing Inspect Controller...")
  }

  ///////////////////////////////////////////////////////////////////////////
  //    Inspect Functions
  ///////////////////////////////////////////////////////////////////////////

  /**
    * Changes the Inspect sub-tab
    * @@param index the given tab index
    * @@param event the given event
    */
  $scope.changeMainTab = (anIndex: js.UndefOr[Int], anEvent: js.UndefOr[dom.Event]) => anIndex foreach { index =>
    $scope.inspectTab = $scope.inspectTabs(index)
    anEvent.foreach(_.preventDefault())
  }

  /**
    * Expands a broker
    */
  $scope.expandBroker = (aBroker: js.UndefOr[TopicDetails]) => aBroker foreach { broker =>
    broker.expanded = !broker.expanded.isTrue
  }

  $scope.expandConsumer = (aConsumer: js.UndefOr[XConsumer]) => {
    for {
      consumer <- aConsumer
      consumerId <- consumer.consumerId
    } {
      consumer.expanded = !consumer.expanded.isTrue
      if (consumer.expanded.isTrue) {
        consumer.loading = true
        consumerGroupService.getConsumer(consumerId) onComplete {
          case Success(consumerUpdate) =>
            $timeout(() => consumer.loading = false, 500.millis)
            $scope.$apply { () =>
              consumer.offsets = consumerUpdate.offsets
              consumer.owners = consumerUpdate.owners
              consumer.threads = consumerUpdate.threads
              consumer.topics = consumerUpdate.offsets.map(_.groupBy(_.topic.orNull).toJSArray map { case (topic, topicOffsets) =>
                new ConsumerTopics(topic, topicOffsets)
              })
              if (consumer.topics.exists(_.length == 1)) {
                consumer.topics.foreach(_.headOption.foreach(_.expanded = true))
              }
            }
          case Failure(e) =>
            $scope.$apply(() => consumer.loading = false)
            errorPopup("Failed to retrieve consumer offsets", e)
        }
      }
    }
  }

  $scope.getConsumerHost = (aConsumer: js.UndefOr[Consumer], aCOffset: js.UndefOr[ConsumerOffset]) => {
    for {
      consumer <- aConsumer
      coffset <- aCOffset
      owners <- consumer.owners
      cowner <- owners.find(o => o.topic == coffset.topic && o.partition == coffset.partition).orUndefined
      threadId <- cowner.threadId
      hostName <- getThreadHostName(consumer.consumerId, threadId)
    } yield hostName
  }

  private def getThreadHostName(aConsumerId: js.UndefOr[String], aThreadId: js.UndefOr[String]) = {
    for {
      consumerId <- aConsumerId
      threadId <- aThreadId
    } yield {
      var name = if (threadId.startsWith(consumerId)) threadId.substring(consumerId.length) else threadId
      while (name.startsWith("_")) name = name.drop(1)

      (1 to 3).foreach { _ =>
        name.lastIndexOf("-") match {
          case -1 =>
          case limit => name = name.substring(0, limit)
        }
      }
      name
    }
  }

  $scope.getConsumerVersion = (aConsumer: js.UndefOr[Consumer], aCOffset: js.UndefOr[ConsumerOffset]) => {
    for {
      consumer <- aConsumer
      coffset <- aCOffset
      owners <- consumer.owners
      cowner <- owners.find(o => o.topic == coffset.topic && o.partition == coffset.partition).orUndefined
      threadId <- cowner.threadId

      threads <- consumer.threads
      thread <- threads.find(t => threadId.startsWith(t.threadId.getOrElse(""))).orUndefined
      version <- thread.version.map(_.toString)
    } yield $scope.versions.getOrElse(version, version)
  }

  $scope.fixThreadName = (aConsumerId: js.UndefOr[String], aThreadId: js.UndefOr[String]) => {
    for {
      consumerId <- aConsumerId
      threadId <- aThreadId
    } yield {
      var name = if (threadId.startsWith(consumerId)) threadId.substring(consumerId.length) else threadId
      while (name.startsWith("_")) name = name.drop(1)
      name
    }
  }

  $scope.getMessagesLeft = (aCOffset: js.UndefOr[ConsumerOffset]) => {
    for {
      coffset <- aCOffset
      offset <- coffset.offset
      topicOffset <- coffset.topicEndOffset
    } yield Math.max(topicOffset - offset, 0)
  }

  $scope.getTotalMessageCount = (aTopicName: js.UndefOr[String]) => {
    for {
      topicName <- aTopicName
      topic <- $scope.topics.find(_.topic == topicName).orUndefined
    } yield topic.totalMessages
  }

  /**
    * Expands the first Zookeeper item
    */
  $scope.expandFirstItem = () => {
    // load the children for the root key
    $scope.zkItems.headOption foreach { firstItem =>
      $scope.expandItem(firstItem)
      $scope.getItemInfo(firstItem)
    }
  }

  /**
    * Expands or collapses the given Zookeeper item
    * @@param item the given Zookeeper item
    */
  $scope.expandItem = (anItem: js.UndefOr[ZkItem]) => anItem foreach { item =>
    item.expanded = !item.expanded.isTrue
    if (item.expanded.isTrue) {
      item.loading = true
      zookeeperService.getZkPath(item.path).withGlobalLoading.withTimer("Retrieving Zookeeper path") onComplete {
        case Success(zkItems) =>
          $scope.$apply { () =>
            item.loading = false
            item.children = zkItems
          }
        case Failure(e) =>
          $scope.$apply(() => item.loading = false)
          errorPopup("Error retrieving Zookeeper data", e)
      }
    }
  }

  $scope.formatData = (aPath: js.UndefOr[String], aFormat: js.UndefOr[String]) => {
    for {
      path <- aPath
      format <- aFormat
    } {
      console.log("path => %s, format = %s", path, format)
      zookeeperService.getZkData(path, format).withGlobalLoading.withTimer("Retrieving Zookeeper data") onComplete {
        case Success(data) =>
          $scope.$apply { () =>
            $scope.zkItem.foreach(_.data = data)
            if (format == "auto") {
              console.log("data => %s", angular.toJson(data))
              data.`type`.foreach($scope.selected.format = _)
            }
          }
        case Failure(e) =>
          errorPopup("Error formatting data", e)
      }
    }
  }

  $scope.getItemInfo = (anItem: js.UndefOr[ZkItem]) => anItem foreach { item =>
    item.loading = true
    zookeeperService.getZkInfo(item.path).withGlobalLoading.withTimer("Retrieving Zookeeper item") onComplete {
      case Success(itemInfo) =>
        $scope.$apply { () =>
          item.loading = false
          //$scope.selected.format = $scope.formats[0]
          $scope.zkItem = itemInfo
        }
      case Failure(e) =>
        $scope.$apply { () => item.loading = false }
        errorPopup("Error loading Zookeeper data", e)
    }
  }

  $scope.expandReplicas = (aTopic: js.UndefOr[TopicDetails]) => aTopic foreach { topic =>
    topic.replicaExpanded = !topic.replicaExpanded.isTrue
    if (topic.replicaExpanded.isTrue) {
      topic.loading = true
      brokerService.getReplicas(topic.topic).withGlobalLoading.withTimer("Retrieving replicas") onComplete {
        case Success(replicas) =>
          $timeout(() => topic.loading = false, 0.5.seconds)
          $scope.$apply { () =>
            topic.replicas = replicas
            replicas.foreach(r => r.inSyncPct = computeInSyncPct(r))
          }
        case Failure(e) =>
          $scope.$apply { () => topic.loading = false }
          errorPopup("Error loading replica data", e)
      }
    }
  }

  $scope.getInSyncClass = (anInSyncPct: js.UndefOr[Double]) => anInSyncPct map {
    case pct if pct == 0 => "in_sync_red"
    case pct if pct == 100 => "in_sync_green"
    case _ => "in_sync_yellow"
  }

  $scope.getInSyncBulbImage = (anInSyncPct: js.UndefOr[Int]) => anInSyncPct foreach {
    case inSyncPct if inSyncPct == 0 => "/assets/images/status/redlight.png"
    case inSyncPct if inSyncPct == 100 => "/assets/images/status/greenlight.png"
    case _ => "/assets/images/status/yellowlight.gif"
  }

  $scope.isConsumerUpToDate = (aConsumer: js.UndefOr[Consumer]) => {
    aConsumer.exists(_.isUpdatedSince(5.minutes))
  }

  private def computeInSyncPct(replicaPartition: ReplicaGroup) = {
    val replicas = replicaPartition.replicas getOrElse emptyArray
    val syncCount = replicas.count(_.inSync.isTrue)
    Math.round(100 * syncCount / replicas.length)
  }

  ///////////////////////////////////////////////////////////////////////////
  //    Event Handler Functions
  ///////////////////////////////////////////////////////////////////////////

  /**
    * Initialize the controller once the reference data has completed loading
    */
  $scope.onReferenceDataLoaded { _ => $scope.init() }

}

/**
  * Inspect Controller
  * @author lawrence.daniels@gmail.com
  */
object InspectController {

  /**
    * Inspect Route Parameters
    * @author lawrence.daniels@gmail.com
    */
  @js.native
  trait InspectRouteParams extends js.Object {
    var mode: js.UndefOr[String] = js.native

  }

  /**
    * Inspect Controller Scope
    * @author lawrence.daniels@gmail.com
    */
  @js.native
  trait InspectScope extends Scope
    with GlobalDataAware with GlobalLoading with GlobalErrorHandling with GlobalNavigation
    with ReferenceDataAware {

    // properties
    var formats: js.Array[String] = js.native
    var inspectTab: MainTab = js.native
    var inspectTabs: js.Array[MainTab] = js.native
    var selected: FormatSelection = js.native
    var versions: js.Dictionary[String] = js.native
    var zkItem: js.UndefOr[ZkItem] = js.native
    var zkItems: js.Array[ZkItem] = js.native

    // functions
    var init: js.Function0[Unit] = js.native
    var changeInspectTab: js.Function2[js.UndefOr[MainTab], js.UndefOr[dom.Event], Unit] = js.native
    var changeMainTab: js.Function2[js.UndefOr[Int], js.UndefOr[dom.Event], Unit] = js.native
    var expandBroker: js.Function1[js.UndefOr[TopicDetails], Unit] = js.native
    var expandFirstItem: js.Function0[Unit] = js.native
    var expandItem: js.Function1[js.UndefOr[ZkItem], Unit] = js.native
    var expandReplicas: js.Function1[js.UndefOr[TopicDetails], Unit] = js.native
    var expandConsumer: js.Function1[js.UndefOr[XConsumer], Unit] = js.native
    var fixThreadName: js.Function2[js.UndefOr[String], js.UndefOr[String], js.UndefOr[String]] = js.native
    var formatData: js.Function2[js.UndefOr[String], js.UndefOr[String], Unit] = js.native
    var getConsumerHost: js.Function2[js.UndefOr[Consumer], js.UndefOr[ConsumerOffset], js.UndefOr[String]] = js.native
    var getConsumerVersion: js.Function2[js.UndefOr[Consumer], js.UndefOr[ConsumerOffset], js.UndefOr[String]] = js.native
    var getInSyncBulbImage: js.Function1[js.UndefOr[Int], Unit] = js.native
    var getInSyncClass: js.Function1[js.UndefOr[Double], js.UndefOr[String]] = js.native
    var getItemInfo: js.Function1[js.UndefOr[ZkItem], Unit] = js.native
    var getMessagesLeft: js.Function1[js.UndefOr[ConsumerOffset], js.UndefOr[Double]] = js.native
    var getTotalMessageCount: js.Function1[js.UndefOr[String], js.UndefOr[Int]] = js.native
    var isActiveInspectTab: js.Function1[js.UndefOr[MainTab], Boolean] = js.native
    var isConsumerUpToDate: js.Function1[js.UndefOr[Consumer], Boolean] = js.native

  }

  @ScalaJSDefined
  trait XConsumer extends Consumer {
    var topics: js.UndefOr[js.Array[ConsumerTopics]] = js.undefined
  }

  @ScalaJSDefined
  class ConsumerTopics(var topic: String, var offsets: js.Array[ConsumerOffset]) extends js.Object {
    var expanded: js.UndefOr[Boolean] = js.undefined
  }

}