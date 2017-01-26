package com.github.ldaniels528.trifecta.sjs.controllers

import com.github.ldaniels528.trifecta.sjs.controllers.GlobalLoading._
import com.github.ldaniels528.trifecta.sjs.controllers.ObserveController._
import com.github.ldaniels528.trifecta.sjs.models.SamplingStatus._
import com.github.ldaniels528.trifecta.sjs.models._
import com.github.ldaniels528.trifecta.sjs.services._
import io.scalajs.npm.angularjs.AngularJsHelper._
import io.scalajs.npm.angularjs._
import io.scalajs.npm.angularjs.toaster.Toaster
import io.scalajs.dom.html.browser.console
import io.scalajs.jquery.JQuery._
import io.scalajs.util.JsUnderOrHelper._
import io.scalajs.util.OptionHelper._
import io.scalajs.util.PromiseHelper._

import scala.concurrent.duration._
import scala.scalajs.concurrent.JSExecutionContext.Implicits.queue
import scala.scalajs.js
import scala.scalajs.js.JSConverters._
import scala.scalajs.js.annotation.ScalaJSDefined
import scala.util.{Failure, Success}

/**
  * Observe Controller
  * @author lawrence.daniels@gmail.com
  */
case class ObserveController($scope: ObserveScope, $interval: Interval, $location: Location, $log: Log, $parse: Parse,
                             $routeParams: ObserveRouteParams, $timeout: Timeout, toaster: Toaster,
                             @injected("MessageDataService") messageDataService: MessageDataService,
                             @injected("MessageSearchService") messageSearchService: MessageSearchService,
                             @injected("QueryService") queryService: QueryService,
                             @injected("TopicService") topicService: TopicService,
                             @injected("ServerSideEventsService") sseSvc: ServerSideEventsService)
  extends Controller with PopupMessages {

  implicit val scope: Scope with GlobalLoading = $scope
  private var lastOffsetChange: Double = 0
  private var delayedLoading = false

  ///////////////////////////////////////////////////////////////////////////
  //    Properties
  //////////////////////////////////////////////////////////////////////////

  $scope.message = js.undefined
  $scope.displayMode = new DisplayMode(state = "message", format = "json")
  $scope.sampling = SamplingStatus(status = SAMPLING_STATUS_STOPPED)

  ///////////////////////////////////////////////////////////////////////////
  //    Initialization Functions
  ///////////////////////////////////////////////////////////////////////////

  private def init() = {
    console.log("Initializing Observe Controller...")
    applyParameters()
  }

  private def applyParameters() = {
    val params = for {
      topicId <- $routeParams.topic.flat
      partitionId <- $routeParams.partition.flat.map(_.toInt)
      offset <- $routeParams.offset.flat.map(_.toInt)
    } yield (topicId, partitionId, offset)

    params.toOption match {
      case Some((topicId, partitionId, offset)) =>
        $scope.moveToMessage(topicId, partitionId, offset)
      case None =>
        $scope.updatePartition(
          $scope.topic.flatMap(_.partitions.sortBy(_.partition.getOrElse(0)).find(_.messages.exists(_ > 0)).orUndefined),
          js.undefined
        )
    }
  }

  ///////////////////////////////////////////////////////////////////////////
  //    Public Functions
  ///////////////////////////////////////////////////////////////////////////

  private def clearMessage() = $scope.message = js.undefined

  /**
    * Converts the given offset from a string value to an integer
    * @@param partition the partition that the offset value will be updated within
    */
  $scope.updatePartitionOffset = (aPartition: js.UndefOr[PartitionDetails]) => {
    for {
      partition <- aPartition
      offset <- partition.offset
    } {
      lastOffsetChange = js.Date.now()
      if(!delayedLoading) delayedMessageLoading()
    }
  }

  private def delayedMessageLoading(): Unit = {
    js.Date.now() - lastOffsetChange match {
      case t if t >= 200 && t < 1000 =>
        delayedLoading = false
        loadMessage()
      case t if t < 200 => $timeout(() => {
        delayedLoading = true
        delayedMessageLoading()
      }, 25.millis)
      case _ =>
        delayedLoading = false
    }
  }

  private def ensureOffset(aPartition: js.UndefOr[PartitionDetails]) = aPartition foreach { partition =>
    if (partition.offset.isEmpty) partition.offset = partition.endOffset.map(_ - 1)
  }

  /**
    * Exports the given message to an external system
    * @@param topic the given topic
    * @@param partition the given partition
    * @@param offset the given offset
    */
  $scope.exportMessage = (topic: js.UndefOr[TopicDetails], partition: js.UndefOr[Int], offset: js.UndefOr[Int]) => {
    toaster.info("Not yet implemented")
  }

  /**
    * Retrieves message data for the given offset within the topic partition.
    * @@param topic the given topic
    * @@param partition the given partition
    * @@param offset the given offset
    */
  $scope.getMessageData = (aTopic: js.UndefOr[String], aPartition: js.UndefOr[Int], anOffset: js.UndefOr[Int]) => {
    for {
      topic <- aTopic
      partition <- aPartition
      offset <- anOffset
    } {
      clearMessage()
      val useDecoder = $scope.getDecodingState(topic).isTrue
      messageDataService.getMessageData(topic, partition, offset, useDecoder).withGlobalLoading.withTimer("Retrieving message data") onComplete {
        case Success(message) =>
          $scope.$apply { () =>
            $scope.message = message
            $location.search(ObserveRouteParams(topic, partition, offset))
          }
        case Failure(e) =>
          errorPopup(e.displayMessage)
      }
    }
  }

  /**
    * Retrieves message key for the given offset within the topic partition.
    * @@param topic the given topic
    * @@param partition the given partition
    * @@param offset the given offset
    */
  $scope.getMessageKey = (aTopic: js.UndefOr[String], aPartition: js.UndefOr[Int], anOffset: js.UndefOr[Int]) => {
    clearMessage()
    for {
      topic <- aTopic
      partition <- aPartition
      offset <- anOffset
    } {
      val useDecoder = $scope.getDecodingState(topic).isTrue
      messageDataService.getMessageKey(topic, partition, offset, useDecoder) onComplete {
        case Success(message) =>
          $scope.$apply { () =>
            $scope.message = message
            $location.search(ObserveRouteParams(topic, partition, offset))
          }
        case Failure(e) =>
          errorPopup(e.displayMessage)
      }
    }
  }

  $scope.setMessageData = (aMessage: js.UndefOr[Message]) => {
    $scope.message = aMessage

    for {
      message <- aMessage
      topic <- $scope.topic
      partitionId <- message.partition
      partition <- topic(partitionId).orUndefined
    } {
      // update the partition with the offset
      $scope.partition = partition
      topic.replace(message)
    }
  }

  $scope.getRemainingCount = (aPartition: js.UndefOr[PartitionDetails]) => {
    for {
      p <- aPartition
      offset <- p.offset
      endOffset <- p.endOffset
    } yield Math.max(endOffset - offset, 0)
  }

  $scope.isSelected = (aPartition: js.UndefOr[PartitionDetails]) => {
    aPartition exists (_.partition ?== $scope.partition.flatMap(_.partition))
  }

  $scope.messageFinderPopup = () => {
    messageSearchService.finderDialog().withGlobalLoading onComplete {
      case Success(form) =>
        console.log(s"form = ${angular.toJson(form)}")

        // perform the validation of the form
        if (form.topic.isEmpty) errorPopup("No topic selected")
        else if (form.criteria.isEmpty) errorPopup("No criteria specified")
        else {
          // display the loading dialog
          val loadingDialog = messageSearchService.loadingDialog()

          for {
            topic <- form.topic.map(_.topic)
            criteria <- form.criteria
          } {
            // perform the search
            queryService.findOne(topic, criteria) onComplete {
              case Success(message) =>
                $scope.$apply { () =>
                  $scope.message = message

                  // find the topic and partition
                  for {
                    myTopic <- $scope.findTopicByName(topic)
                    partitionID <- message.partition
                    myPartition <- myTopic(partitionID)
                  } {
                    $scope.topic = myTopic
                    $scope.partition = myPartition
                    $scope.partition.foreach(_.offset = message.offset)
                  }
                }
              case Failure(e) =>
                errorPopup(e.displayMessage)
            }
          }
        }
      case Failure(e) =>
        errorPopup(e.displayMessage)
    }
  }

  $scope.gotoDecoder = (aTopic: js.UndefOr[TopicDetails]) => {
    val scope = angular.element($("#Decoders")).scope().asInstanceOf[ObserveController]
    // TODO switchToDecoderByTopic?
    /*
    if (scope.switchToDecoderByTopic(topic)) {
      $scope.changeTab(4, null) // Decoders
    }*/
  }

  $scope.isLimitedControls = () => $scope.sampling.status.contains(SAMPLING_STATUS_STARTED)

  private def loadMessage() = {
    for {
      topic <- $scope.topic.map(_.topic)
      partition <- $scope.partition.map(_.partition)
      offset <- $scope.partition.map(_.offset)
    } {
      console.log(s"Loading message $topic:$partition@$offset ...")
      $scope.displayMode.state match {
        case "key" => $scope.getMessageKey(topic, partition, offset)
        case "message" => $scope.getMessageData(topic, partition, offset)
        case mode =>
          console.warn(s"Unrecognized display mode (mode = $mode)")
          $scope.getMessageData(topic, partition, offset)
      }
    }
  }

  $scope.firstMessage = () => {
    ensureOffset($scope.partition)
    for {
      partition <- $scope.partition
      offset <- partition.offset
      startOffset <- partition.startOffset
    } {
      if (offset != startOffset) {
        partition.offset = startOffset
        loadMessage()
      }
    }
  }

  $scope.lastMessage = () => {
    ensureOffset($scope.partition)
    for {
      partition <- $scope.partition
      offset <- partition.offset
      endOffset <- partition.endOffset.map(offset => Math.max(0, offset - 1))
    } {
      if (offset != endOffset) {
        partition.offset = endOffset
        loadMessage()
      }
    }
  }

  $scope.medianMessage = () => {
    ensureOffset($scope.partition)
    for {
      partition <- $scope.partition
      offset <- partition.offset
      startOffset <- partition.startOffset
      endOffset <- partition.endOffset
    } {
      val median = Math.round(startOffset + (endOffset - startOffset) / 2L)
      if (offset != median) {
        partition.offset = median
        loadMessage()
      }
    }
  }

  $scope.messageSamplingStart = (aTopic: js.UndefOr[TopicDetails]) => aTopic foreach { topic =>
    val partitionOffsets = topic.partitions map { p =>
      p.offset getOrElse (p.endOffset getOrElse 0)
    }

    sseSvc.startSampling(topic.topic, partitionOffsets).withGlobalLoading.withTimer("Start sampling") onComplete {
      case Success(response) =>
        $scope.sampling.sessionId = response.sessionId
        $scope.sampling.status = SAMPLING_STATUS_STARTED
      case Failure(e) =>
        toaster.error("Failed to start message sampling")
    }
  }

  $scope.messageSamplingStop = (aTopic: js.UndefOr[TopicDetails]) => aTopic foreach { topic =>
    $scope.sampling.sessionId.toOption match {
      case Some(sessionId) =>
        sseSvc.stopSampling(sessionId).withGlobalLoading.withTimer("Stop sampling") onComplete {
          case Success(response) =>
            $scope.sampling.status = SAMPLING_STATUS_STOPPED
          case Failure(e) =>
            toaster.error("Failed to stop message sampling")
        }
      case None =>
        toaster.warning("No streaming session found")
    }
  }

  $scope.moveToFirstMessage = (aTopicID: js.UndefOr[String], aPartitionID: js.UndefOr[Int]) => {
    for {
      topicID <- aTopicID
      topic <- $scope.findTopicByName(topicID)
      partitionID <- aPartitionID
      partition <- topic.partitions.find(_.partition == partitionID).orUndefined
      firstOffset <- partition.startOffset
    } {
      $scope.updateTopic(topic, aPartitionID, firstOffset)
    }
  }

  $scope.moveToLastMessage = (aTopicID: js.UndefOr[String], aPartitionID: js.UndefOr[Int]) => {
    for {
      topicID <- aTopicID
      topic <- $scope.findTopicByName(topicID)
      partitionID <- aPartitionID
      partition <- topic.partitions.find(_.partition == partitionID).orUndefined
      lastOffset <- partition.endOffset.map(offset => Math.max(0, offset - 1))
    } {
      $scope.updateTopic(topic, aPartitionID, lastOffset)
    }
  }

  $scope.moveToMessage = (aTopicID: js.UndefOr[String], aPartitionID: js.UndefOr[Int], anOffset: js.UndefOr[Int]) => {
    for {
      topicID <- aTopicID
      topic <- $scope.findTopicByName(topicID)
    } {
      $scope.updateTopic(topic, aPartitionID, anOffset)
    }
  }

  $scope.nextMessage = () => {
    ensureOffset($scope.partition)
    for {
      partition <- $scope.partition
      offset <- partition.offset
      endOffset <- partition.endOffset
    } {
      if (offset < endOffset) $scope.partition.foreach(p => p.offset = p.offset.map(_ + 1))
      loadMessage()
    }
  }

  $scope.previousMessage = () => {
    ensureOffset($scope.partition)
    for {
      partition <- $scope.partition
      offset <- partition.offset
      startOffset <- partition.startOffset
    } {
      if (offset > startOffset) $scope.partition.foreach(p => p.offset = p.offset.map(_ - 1))
      loadMessage()
    }
  }

  $scope.resetMessageState = (aMode: js.UndefOr[String], aTopic: js.UndefOr[String], aPartition: js.UndefOr[Int], anOffset: js.UndefOr[Int]) => {
    for {
      mode <- aMode
      topic <- aTopic
      partition <- aPartition
      offset <- anOffset
    } {
      mode match {
        case "key" => $scope.getMessageKey(topic, partition, offset)
        case "message" => $scope.getMessageData(topic, partition, offset)
        case _ =>
          console.warn(s"Unrecognized display mode (mode = $mode)")
          $scope.getMessageData(topic, partition, offset)
      }
    }
  }

  /**
    * Toggles the Avro/JSON output flag
    */
  $scope.toggleAvroOutput = () => {
    $scope.displayMode.format = if ($scope.displayMode.format == "json") "avro" else "json"
  }

  $scope.toggleDecodeOnOff = (aTopic: js.UndefOr[String], aMessage: js.UndefOr[Message]) => {
    for {
      topic <- aTopic
      message <- aMessage
      partition <- message.partition
      offset <- message.offset
    } {
      $scope.toggleDecodingState(topic)
      $scope.getMessageData(topic, partition, offset)
    }
  }

  $scope.messageAsASCII = (aMessage: js.UndefOr[Message]) => {
    for {
      message <- aMessage
      payload <- message.payload
    } yield payload.toString
  }

  /**
    * Formats a JSON object as a color-coded JSON expression
    * @@param objStr the JSON object
    * @@param tabWidth the number of tabs to use in formatting
    * @return a pretty formatted JSON string
    */
  $scope.messageAsJSON = (aMessage: js.UndefOr[Message], aTabWidth: js.UndefOr[Int]) => {
    for {
      message <- aMessage
      payload <- message.payload
    } yield angular.toJson(payload, pretty = true)
  }

  $scope.updatePartition = (partition: js.UndefOr[PartitionDetails], anOffset: js.UndefOr[Int]) => {
    $scope.partition = partition

    // optionally, set the offset
    partition.foreach(p => p.offset = anOffset ?? p.offset)

    // if the current offset is not set, set it at the starting offset.
    ensureOffset(partition)

    // load the first message
    loadMessage()
  }

  $scope.updateTopic = (aTopic: js.UndefOr[TopicDetails], aPartitionID: js.UndefOr[Int], anOffset: js.UndefOr[Int]) => {
    $scope.selectTopic(aTopic)

    aTopic.map(_.partitions).toOption match {
      case Some(partitions) =>
        //console.log(s"partitions = ${angular.toJson(partitions, pretty = true)}")
        val partition = aPartitionID.toOption.flatMap(id => partitions.find(_.partition == id)) ?? partitions.find(_.messages.exists(_ > 0)) ?? partitions.headOption
        $scope.updatePartition(partition.orUndefined, anOffset)
      // TODO     partition.offset = offset

      // load the message
      case None =>
        warningPopup("No partitions found")
        $scope.partition = js.undefined
        clearMessage()
    }
  }

  ///////////////////////////////////////////////////////////////////////////
  //    Event Handler Functions
  ///////////////////////////////////////////////////////////////////////////

  /**
    * React to incoming message samples
    */
  $scope.onMessageSample { message =>
    // is sampling already running?
    if (!$scope.sampling.status.contains(SAMPLING_STATUS_STARTED)) {
      console.info("Sampling was already running...")
      $scope.$apply(() => $scope.sampling.status = SAMPLING_STATUS_STARTED)
      sseSvc.getSamplingSession onComplete {
        case Success(response) =>
          console.log(s"response => ${angular.toJson(response)}")
          $scope.$apply(() => $scope.sampling.sessionId = response.sessionId)
        case Failure(e) =>
          console.error(s"Failed to read the sampling session: ${e.displayMessage}")
      }
    }

    $scope.$apply(() => $scope.setMessageData(message))
  }

  /**
    * Initialize the controller once the topics have been loaded
    */
  $scope.onTopicsLoaded { _ => init() }

  /**
    * Watch for topic changes, and select the first non-empty topic
    */
  $scope.$watchCollection($scope.topics, (theNewTopics: js.UndefOr[js.Array[TopicDetails]], theOldTopics: js.UndefOr[js.Array[TopicDetails]]) => theNewTopics foreach { newTopics =>
    console.info(s"Loaded new topics (${newTopics.length})")
    if ($scope.topics.forall(_.totalMessages == 0)) $scope.hideEmptyTopics = false
    $scope.updateTopic($scope.findNonEmptyTopic(), js.undefined, js.undefined)
  })

  // did we receive parameters?
  applyParameters()

}

/**
  * Observe Controller Companion
  * @author lawrence.daniels@gmail.com
  */
object ObserveController {

  /**
    * Display Mode
    * @param state  the current display mode (e.g. "key" or "message")
    * @param format the current format
    */
  @ScalaJSDefined
  class DisplayMode(var state: String, var format: String) extends js.Object

  /**
    * Observe Route Parameters
    * @author lawrence.daniels@gmail.com
    */
  @ScalaJSDefined
  class ObserveRouteParams(var topic: js.UndefOr[String],
                           var partition: js.UndefOr[String],
                           var offset: js.UndefOr[String]) extends js.Object

  /**
    * Observe Route Parameters Companion
    * @author lawrence.daniels@gmail.com
    */
  object ObserveRouteParams {
    def apply(topic: String, partition: Int, offset: Int): ObserveRouteParams = {
      new ObserveRouteParams(topic, partition.toString, offset.toString)
    }
  }

  /**
    * Observe Controller Scope
    * @author lawrence.daniels@gmail.com
    */
  @js.native
  trait ObserveScope extends Scope
    with GlobalDataAware with GlobalDecodingState with GlobalErrorHandling with GlobalLoading
    with MainTabManagement with ReferenceDataAware {

    // properties
    var displayMode: DisplayMode = js.native
    var message: js.UndefOr[Message] = js.native
    var sampling: SamplingStatus = js.native
    var partition: js.UndefOr[PartitionDetails] = js.native

    // functions
    var gotoDecoder: js.Function1[js.UndefOr[TopicDetails], Unit] = js.native
    var isLimitedControls: js.Function0[Boolean] = js.native
    var isSelected: js.Function1[js.UndefOr[PartitionDetails], Boolean] = js.native
    var messageAsASCII: js.Function1[js.UndefOr[Message], js.UndefOr[String]] = js.native
    var messageAsJSON: js.Function2[js.UndefOr[Message], js.UndefOr[Int], js.UndefOr[String]] = js.native
    var toggleAvroOutput: js.Function0[Unit] = js.native
    var updatePartition: js.Function2[js.UndefOr[PartitionDetails], js.UndefOr[Int], Unit] = js.native
    var updateTopic: js.Function3[js.UndefOr[TopicDetails], js.UndefOr[Int], js.UndefOr[Int], Unit] = js.native

    // Kafka message functions
    var exportMessage: js.Function3[js.UndefOr[TopicDetails], js.UndefOr[Int], js.UndefOr[Int], Unit] = js.native
    var firstMessage: js.Function0[Unit] = js.native
    var getMessageData: js.Function3[js.UndefOr[String], js.UndefOr[Int], js.UndefOr[Int], Unit] = js.native
    var getMessageKey: js.Function3[js.UndefOr[String], js.UndefOr[Int], js.UndefOr[Int], Unit] = js.native
    var getRemainingCount: js.Function1[js.UndefOr[PartitionDetails], js.UndefOr[Int]] = js.native
    var lastMessage: js.Function0[Unit] = js.native
    var medianMessage: js.Function0[Unit] = js.native
    var messageFinderPopup: js.Function0[Unit] = js.native
    var messageSamplingStart: js.Function1[js.UndefOr[TopicDetails], Unit] = js.native
    var messageSamplingStop: js.Function1[js.UndefOr[TopicDetails], Unit] = js.native
    var moveToFirstMessage: js.Function2[js.UndefOr[String], js.UndefOr[Int], Unit] = js.native
    var moveToLastMessage: js.Function2[js.UndefOr[String], js.UndefOr[Int], Unit] = js.native
    var moveToMessage: js.Function3[js.UndefOr[String], js.UndefOr[Int], js.UndefOr[Int], Unit] = js.native
    var nextMessage: js.Function0[Unit] = js.native
    var previousMessage: js.Function0[Unit] = js.native
    var resetMessageState: js.Function4[js.UndefOr[String], js.UndefOr[String], js.UndefOr[Int], js.UndefOr[Int], Unit] = js.native
    var setMessageData: js.Function1[js.UndefOr[Message], Unit] = js.native
    var updatePartitionOffset: js.Function1[js.UndefOr[PartitionDetails], Unit] = js.native
    var toggleDecodeOnOff: js.Function2[js.UndefOr[String], js.UndefOr[Message], Unit] = js.native
  }

}