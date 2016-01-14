package com.github.ldaniels528.trifecta.sjs.controllers

import com.github.ldaniels528.scalascript._
import com.github.ldaniels528.scalascript.core._
import com.github.ldaniels528.scalascript.extensions.Toaster
import com.github.ldaniels528.scalascript.util.ScalaJsHelper._
import com.github.ldaniels528.trifecta.sjs.controllers.GlobalLoading._
import com.github.ldaniels528.trifecta.sjs.controllers.ReferenceDataAware._
import com.github.ldaniels528.trifecta.sjs.models.SamplingStatus._
import com.github.ldaniels528.trifecta.sjs.models._
import com.github.ldaniels528.trifecta.sjs.services.ServerSideEventsService._
import com.github.ldaniels528.trifecta.sjs.services._
import org.scalajs.dom
import org.scalajs.dom.console
import org.scalajs.jquery.jQuery

import scala.scalajs.concurrent.JSExecutionContext.Implicits.runNow
import scala.scalajs.js
import scala.scalajs.js.JSConverters._
import scala.util.{Failure, Success}

/**
  * Observe Controller
  * @author lawrence.daniels@gmail.com
  */
class ObserveController($scope: ObserveControllerScope, $interval: Interval, $parse: Parse, $timeout: Timeout, toaster: Toaster,
                        @injected("MessageSvc") messageSvc: MessageDataService,
                        @injected("MessageSearchSvc") messageSearchSvc: MessageSearchService,
                        @injected("QuerySvc") querySvc: QueryService,
                        @injected("TopicSvc") topicSvc: TopicService,
                        @injected("ServerSideEventsSvc") sseSvc: ServerSideEventsService)
  extends Controller {

  implicit val scope = $scope

  ///////////////////////////////////////////////////////////////////////////
  //    Properties
  //////////////////////////////////////////////////////////////////////////

  $scope.message = js.undefined
  $scope.displayMode = DisplayMode(state = "message", avro = "json")
  $scope.sampling = SamplingStatus(status = SAMPLING_STATUS_STOPPED)

  ///////////////////////////////////////////////////////////////////////////
  //    Initialization Functions
  ///////////////////////////////////////////////////////////////////////////

  $scope.init = () => {
    console.log("Initializing Observe Controller...")
    $scope.updatePartition($scope.topic.flatMap(_.partitions.sortBy(_.partition.getOrElse(0)).find(_.messages.exists(_ > 0)).orUndefined))
  }

  ///////////////////////////////////////////////////////////////////////////
  //    Public Functions
  ///////////////////////////////////////////////////////////////////////////

  $scope.clearMessage = () => $scope.message = js.undefined

  $scope.isSelected = (aPartition: js.UndefOr[PartitionDetails]) => {
    aPartition exists (_.partition ?== $scope.partition.flatMap(_.partition))
  }

  /**
    * Converts the given offset from a string value to an integer
    * @@param partition the partition that the offset value will be updated within
    * @@param offset the given offset string value
    */
  $scope.convertOffsetToInt = (aPartition: js.UndefOr[PartitionDetails], anOffset: js.UndefOr[Int]) => {
    for {
      partition <- aPartition
      offset <- anOffset
    } partition.offset = offset
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
      $scope.clearMessage()
      messageSvc.getMessage(topic, partition, offset).withGlobalLoading.withTimer("Retrieving message data") onComplete {
        case Success(message) =>
          $scope.message = message
        case Failure(e) =>
          $scope.addErrorMessage(e.displayMessage)
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

  /**
    * Retrieves message key for the given offset within the topic partition.
    * @@param topic the given topic
    * @@param partition the given partition
    * @@param offset the given offset
    */
  $scope.getMessageKey = (aTopic: js.UndefOr[String], aPartition: js.UndefOr[Int], anOffset: js.UndefOr[Int]) => {
    $scope.clearMessage()
    for {
      topic <- aTopic
      partition <- aPartition
      offset <- anOffset
    } {
      messageSvc.getMessageKey(topic, partition, offset) onComplete {
        case Success(message) =>
          $scope.message = message
        case Failure(e) =>
          $scope.addErrorMessage(e.displayMessage)
      }
    }
  }

  $scope.getRemainingCount = (aParition: js.UndefOr[PartitionDetails]) => {
    for {
      p <- aParition
      offset <- p.offset
      endOffset <- p.endOffset
    } yield Math.max(endOffset - offset, 0)
  }

  $scope.messageFinderPopup = () => {
    messageSearchSvc.finderDialog() onComplete {
      case Success(form) =>
        // perform the validation of the form
        if (form.topic.isEmpty) $scope.addErrorMessage("No topic selected")
        else if (form.criteria.isEmpty) $scope.addErrorMessage("No criteria specified")
        else {
          // display the loading dialog
          val loadingDialog = messageSearchSvc.loadingDialog()

          for {
            topic <- form.topic.map(_.topic)
            criteria <- form.criteria
          } {
            // perform the search
            querySvc.findOne(topic, criteria) onComplete {
              case Success(message) =>
                //loadingDialog.close(new js.Object())
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
              case Failure(e) =>
                $scope.addErrorMessage(e.displayMessage)
            }
          }
        }
      case Failure(e) =>
        $scope.addErrorMessage(e.displayMessage)
    }
  }

  $scope.gotoDecoder = (topic: js.UndefOr[TopicDetails]) => {
    val scope = angular.element(jQuery("#Decoders")).scope().asInstanceOf[ObserveController]
    // TODO switchToDecoderByTopic?
    /*
    if (scope.switchToDecoderByTopic(topic)) {
      $scope.changeTab(4, null) // Decoders
    }*/
  }

  $scope.isLimitedControls = () => $scope.sampling.status.contains(SAMPLING_STATUS_STARTED)

  $scope.loadMessage = () => {
    for {
      topic <- $scope.topic.map(_.topic)
      partition <- $scope.partition.map(_.partition)
      offset <- $scope.partition.map(_.offset)
    } {
      $scope.displayMode.state match {
        case "key" => $scope.getMessageKey(topic, partition, offset)
        case "message" => $scope.getMessageData(topic, partition, offset)
        case state =>
          console.error(s"Unrecognized display mode (mode = $state)")
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
        $scope.loadMessage()
      }
    }
  }

  $scope.lastMessage = () => {
    ensureOffset($scope.partition)
    for {
      partition <- $scope.partition
      offset <- partition.offset
      endOffset <- partition.endOffset
    } {
      if (offset != endOffset) {
        partition.offset = endOffset
        $scope.loadMessage()
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
        $scope.loadMessage()
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

  $scope.nextMessage = () => {
    ensureOffset($scope.partition)
    for {
      partition <- $scope.partition
      offset <- partition.offset
      endOffset <- partition.endOffset
    } {
      if (offset < endOffset) $scope.partition.foreach(p => p.offset = p.offset.map(_ + 1))
      $scope.loadMessage()
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
      $scope.loadMessage()
    }
  }

  $scope.resetMessageState = (aMode: js.UndefOr[String], aTopic: js.UndefOr[String], aPartition: js.UndefOr[Int], anOffset: js.UndefOr[Int]) => {
    for {
      mode <- aMode
      topic <- aTopic
      partition <- aPartition
      offset <- anOffset
    } {
      console.info(s"mode = $mode, topic = $topic, partition = $partition, offset = $offset")
      mode match {
        case "key" => $scope.getMessageKey(topic, partition, offset)
        case "message" => $scope.getMessageData(topic, partition, offset)
        case _ =>
          console.error(s"Unrecognized display mode (mode = $mode)")
      }
    }
  }

  $scope.switchToMessage = (aTopic: js.UndefOr[String], aPartition: js.UndefOr[Int], anOffset: js.UndefOr[Int]) => {
    for {
      topicID <- aTopic
      partitionID <- aPartition
      offset <- anOffset
      _ = console.info(s"switchToMessage: topicID = $topicID, partitionID = $partitionID, offset = $offset")
      topic <- $scope.findTopicByName(topicID)
      partition <- topic(partitionID).orUndefined
    } {
      $scope.topic = topic
      $scope.partition = partition
      $scope.partition.foreach(_.offset = offset)
      $scope.loadMessage()
      //$scope.changeTab(1, null) // TODO switch to Observe tab
    }
  }

  /**
    * Toggles the Avro/JSON output flag
    */
  $scope.toggleAvroOutput = () => {
    $scope.displayMode.avro = if ($scope.displayMode.avro == "json") "avro" else "json"
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

  $scope.updatePartition = (partition: js.UndefOr[PartitionDetails]) => {
    $scope.partition = partition

    // if the current offset is not set, set it at the starting offset.
    ensureOffset(partition)

    // load the first message
    $scope.loadMessage()
  }

  $scope.updateTopic = (aTopic: js.UndefOr[TopicDetails]) => {
    $scope.topic = aTopic

    aTopic.toOption.map(_.partitions) match {
      case Some(partitions) =>
        val partition = partitions.head
        $scope.updatePartition(partition)

      // load the message
      //$scope.loadMessage()
      case None =>
        console.log("No partitions found")
        $scope.partition = js.undefined
        $scope.clearMessage()
    }
  }

  private def ensureOffset(aPartition: js.UndefOr[PartitionDetails]) = aPartition foreach { partition =>
    if (partition.offset.isEmpty) partition.offset = partition.endOffset
  }

  ///////////////////////////////////////////////////////////////////////////
  //    Event Handler Functions
  ///////////////////////////////////////////////////////////////////////////

  /**
    * React to incoming message samples
    */
  $scope.$on(MESSAGE_SAMPLE, (event: dom.Event, message: Message) => $scope.$apply(() => {
    // is sampling already running?
    if (!$scope.sampling.status.contains(SAMPLING_STATUS_STARTED)) {
      console.info("Sampling was already running...")
      $scope.sampling.status = SAMPLING_STATUS_STARTED
      sseSvc.getSamplingSession onComplete {
        case Success(response) =>
          console.log(s"response => ${angular.toJson(response)}")
          $scope.sampling.sessionId = response.sessionId
        case Failure(e) =>
          console.error(s"Failed to read the sampling session: ${e.displayMessage}")
      }
    }

    $scope.setMessageData(message)
  }))

  /**
    * Initialize the controller once the reference data has completed loading
    */
  $scope.$on(REFERENCE_DATA_LOADED, (event: dom.Event, data: ReferenceData) => $scope.init())

  /**
    * Watch for topic changes, and select the first non-empty topic
    */
  $scope.$watchCollection($scope.topics, (theNewTopics: js.UndefOr[js.Array[TopicDetails]], theOldTopics: js.UndefOr[js.Array[TopicDetails]]) => theNewTopics foreach { newTopics =>
    console.info(s"Loaded new topics (${newTopics.length})")
    if ($scope.topics.forall(_.totalMessages == 0)) $scope.hideEmptyTopics = false
    $scope.updateTopic($scope.findNonEmptyTopic())
  })

}

/**
  * Observe Controller Scope
  * @author lawrence.daniels@gmail.com
  */
@js.native
trait ObserveControllerScope extends Scope with GlobalDataAware with GlobalErrorHandling with GlobalLoading with MainTabManagement with ReferenceDataAware {
  // properties
  var displayMode: DisplayMode = js.native
  var message: js.UndefOr[Message] = js.native
  var sampling: SamplingStatus = js.native
  var partition: js.UndefOr[PartitionDetails] = js.native

  // functions
  var init: js.Function0[Unit] = js.native
  var convertOffsetToInt: js.Function2[js.UndefOr[PartitionDetails], js.UndefOr[Int], Unit] = js.native
  var gotoDecoder: js.Function1[js.UndefOr[TopicDetails], Unit] = js.native
  var isLimitedControls: js.Function0[Boolean] = js.native
  var isSelected: js.Function1[js.UndefOr[PartitionDetails], Boolean] = js.native
  var messageAsJSON: js.Function2[js.UndefOr[Message], js.UndefOr[Int], js.UndefOr[String]] = js.native
  var toggleAvroOutput: js.Function0[Unit] = js.native
  var updatePartition: js.Function1[js.UndefOr[PartitionDetails], Unit] = js.native
  var updateTopic: js.Function1[js.UndefOr[TopicDetails], Unit] = js.native

  // Kafka message functions
  var clearMessage: js.Function0[Unit] = js.native
  var exportMessage: js.Function3[js.UndefOr[TopicDetails], js.UndefOr[Int], js.UndefOr[Int], Unit] = js.native
  var firstMessage: js.Function0[Unit] = js.native
  var getMessageData: js.Function3[js.UndefOr[String], js.UndefOr[Int], js.UndefOr[Int], Unit] = js.native
  var getMessageKey: js.Function3[js.UndefOr[String], js.UndefOr[Int], js.UndefOr[Int], Unit] = js.native
  var getRemainingCount: js.Function1[js.UndefOr[PartitionDetails], js.UndefOr[Int]] = js.native
  var lastMessage: js.Function0[Unit] = js.native
  var loadMessage: js.Function0[Unit] = js.native
  var medianMessage: js.Function0[Unit] = js.native
  var messageFinderPopup: js.Function0[Unit] = js.native
  var messageSamplingStart: js.Function1[js.UndefOr[TopicDetails], Unit] = js.native
  var messageSamplingStop: js.Function1[js.UndefOr[TopicDetails], Unit] = js.native
  var nextMessage: js.Function0[Unit] = js.native
  var previousMessage: js.Function0[Unit] = js.native
  var resetMessageState: js.Function4[js.UndefOr[String], js.UndefOr[String], js.UndefOr[Int], js.UndefOr[Int], Unit] = js.native
  var setMessageData: js.Function1[js.UndefOr[Message], Unit] = js.native
  var switchToMessage: js.Function3[js.UndefOr[String], js.UndefOr[Int], js.UndefOr[Int], Unit] = js.native
}
