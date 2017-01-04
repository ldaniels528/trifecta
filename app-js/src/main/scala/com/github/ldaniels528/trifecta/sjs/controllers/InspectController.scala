package com.github.ldaniels528.trifecta.sjs.controllers

import com.github.ldaniels528.trifecta.sjs.controllers.GlobalLoading._
import com.github.ldaniels528.trifecta.sjs.controllers.InspectController._
import com.github.ldaniels528.trifecta.sjs.models._
import com.github.ldaniels528.trifecta.sjs.services.{TopicService, ZookeeperService}
import org.scalajs.angularjs._
import org.scalajs.angularjs.toaster.Toaster
import org.scalajs.dom
import org.scalajs.dom.browser.console
import org.scalajs.nodejs.util.ScalaJsHelper._
import org.scalajs.sjs.JsUnderOrHelper._
import org.scalajs.sjs.PromiseHelper._

import scala.concurrent.duration._
import scala.scalajs.concurrent.JSExecutionContext.Implicits.queue
import scala.scalajs.js
import scala.util.{Failure, Success}

/**
  * Inspect Controller
  * @author lawrence.daniels@gmail.com
  */
case class InspectController($scope: InspectScope, $location: Location, $log: Log, $routeParams: InspectRouteParams,
                             $timeout: Timeout, $interval: Interval, toaster: Toaster,
                             @injected("TopicService") topicService: TopicService,
                             @injected("ZookeeperService") zookeeperService: ZookeeperService)
  extends Controller with PopupMessages {

  implicit val scope: Scope with GlobalLoading = $scope

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

  /**
    * Expands the consumers for the given topic
    * @@param topic the given topic
    */
  $scope.expandTopicConsumers = (aTopic: js.UndefOr[TopicDetails]) => aTopic foreach { topic =>
    topic.expanded = !topic.expanded.isTrue
    if (topic.expanded.isTrue) {
      topic.loadingConsumers = true
      topicService.getConsumerGroups(topic.topic).withGlobalLoading.withTimer("Retrieving consumers by topic") onComplete {
        case Success(consumerGroups) =>
          $scope.$apply { () =>
            topic.loadingConsumers = false
            updateConsumerGroups(consumerGroups)
          }
        case Failure(e) =>
          topic.loadingConsumers = false
          errorPopup("Failed to retrieve consumers", e)
      }
    }
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

  private def updateConsumerGroups(consumerGroups: js.Array[ConsumerGroup]) {
    consumerGroups foreach { group =>
      group.details foreach { detail =>
        $scope.consumers.find(c =>
          (c.consumerId ?== group.consumerId) &&
            (c.topic ?== detail.topic) &&
            (c.partition ?== detail.partition)) match {
          case Some(consumer) =>
            consumer.update(detail)
          case None =>
            $scope.consumers.push(detail)
            $scope.consumerGroupCache.clear()
        }
      }
    }
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
      topicService.getReplicas(topic.topic).withGlobalLoading.withTimer("Retrieving replicas") onComplete {
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
    var expandTopicConsumers: js.Function1[js.UndefOr[TopicDetails], Unit] = js.native
    var fixThreadName: js.Function2[js.UndefOr[String], js.UndefOr[String], js.UndefOr[String]] = js.native
    var formatData: js.Function2[js.UndefOr[String], js.UndefOr[String], Unit] = js.native
    var getInSyncBulbImage: js.Function1[js.UndefOr[Int], Unit] = js.native
    var getInSyncClass: js.Function1[js.UndefOr[Double], js.UndefOr[String]] = js.native
    var getItemInfo: js.Function1[js.UndefOr[ZkItem], Unit] = js.native
    var isActiveInspectTab: js.Function1[js.UndefOr[MainTab], Boolean] = js.native
    var isConsumerUpToDate: js.Function1[js.UndefOr[Consumer], Boolean] = js.native

  }

}