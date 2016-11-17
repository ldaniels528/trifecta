package com.github.ldaniels528.trifecta.sjs.controllers

import PublishController._
import java.util.UUID

import com.github.ldaniels528.trifecta.sjs.controllers.GlobalLoading._
import com.github.ldaniels528.trifecta.sjs.models.MessageBlob
import com.github.ldaniels528.trifecta.sjs.services.{MessageDataService, TopicService}
import org.scalajs.angularjs.AngularJsHelper._
import org.scalajs.angularjs._
import org.scalajs.angularjs.toaster.Toaster
import org.scalajs.dom.browser.console
import org.scalajs.sjs.PromiseHelper._

import scala.scalajs.concurrent.JSExecutionContext.Implicits.queue
import scala.scalajs.js
import scala.util.{Failure, Success}

/**
  * Publish Controller
  * @author lawrence.daniels@gmail.com
  */
class PublishController($scope: PublishScope, $log: Log, $timeout: Timeout, toaster: Toaster,
                        @injected("MessageDataService") messageDataService: MessageDataService,
                        @injected("TopicService") topicService: TopicService)
  extends Controller {

  implicit val scope = $scope

  $scope.keyFormats = js.Array("ASCII", "Hex-Notation", "EPOC", "UUID")
  $scope.messageFormats = js.Array("ASCII", "Avro", "JSON", "Hex-Notation")

  ///////////////////////////////////////////////////////////////////////////
  //    Initialization Functions
  ///////////////////////////////////////////////////////////////////////////

  $scope.init = () => {
    console.log("Initializing Publish Controller...")
  }

  ///////////////////////////////////////////////////////////////////////////
  //    Publish Functions
  ///////////////////////////////////////////////////////////////////////////

  /**
    * Publishes the message to the topic
    * @@param blob the message object
    */
  $scope.publishMessage = (aBlob: js.UndefOr[MessageBlob]) => aBlob foreach { blob =>
    if (validated(blob)) {
      console.log(s"topic <- ${blob.topic.map(_.topic).orNull}, key <- ${blob.key.orNull}, messageFormat <- ${blob.messageFormat.orNull}")
      for {
        topic <- blob.topic.map(_.topic)
        key = blob.key getOrElse UUID.randomUUID().toString
        message <- blob.message
        keyFormat <- blob.keyFormat
        messageFormat <- blob.messageFormat
      } {
        messageDataService.publishMessage(topic, key, message, keyFormat, messageFormat).withGlobalLoading.withTimer("Publishing message...") onComplete {
          case Success(response) =>
            $scope.$apply { () =>
              $log.info(s"response = ${angular.toJson(response)}")
              toaster.success("Message published")
            }
          case Failure(e) =>
            $scope.$apply { () =>
              $scope.addErrorMessage(e.displayMessage)
            }
        }
      }
    }
  }

  /**
    * Validates the given message blob
    * @param blob the given message blob
    * @return {boolean}
    */
  private def validated(blob: MessageBlob) = {
    val messages = blob.validate
    messages foreach ($scope.addErrorMessage(_))
    messages.isEmpty
  }

}

/**
  * Publish Controller Companion
  * @author lawrence.daniels@gmail.com
  */
object PublishController {

  /**
    * Publish Controller Scope
    * @author lawrence.daniels@gmail.com
    */
  @js.native
  trait PublishScope extends Scope
    with GlobalErrorHandling with GlobalLoading with ReferenceDataAware {
    // properties
    var keyFormats: js.Array[String] = js.native
    var messageFormats: js.Array[String] = js.native

    // functions
    var init: js.Function0[Unit] = js.native
    var publishMessage: js.Function1[js.UndefOr[MessageBlob], Unit] = js.native
  }

}