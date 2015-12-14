package com.github.ldaniels528.trifecta.sjs.controllers

import com.github.ldaniels528.scalascript.core._
import com.github.ldaniels528.scalascript.extensions.Toaster
import com.github.ldaniels528.scalascript.util.ScalaJsHelper._
import com.github.ldaniels528.scalascript.{Controller, Scope, angular, injected}
import com.github.ldaniels528.trifecta.sjs.controllers.GlobalLoading._
import com.github.ldaniels528.trifecta.sjs.models.MessageBlob
import com.github.ldaniels528.trifecta.sjs.services.{MessageDataService, TopicService}
import org.scalajs.dom.console

import scala.scalajs.concurrent.JSExecutionContext.Implicits.runNow
import scala.scalajs.js
import scala.util.{Failure, Success}

/**
  * Publish Controller
  * @author lawrence.daniels@gmail.com
  */
class PublishController($scope: PublishControllerScope, $log: Log, $timeout: Timeout, toaster: Toaster,
                        @injected("MessageSvc") messageSvc: MessageDataService,
                        @injected("TopicSvc") topicSvc: TopicService)
  extends Controller {

  implicit val scope = $scope

  $scope.keyFormats = js.Array("ASCII", "Hex-Notation", "EPOC", "UUID")
  $scope.messageFormats = js.Array("ASCII", "Avro", "JSON", "Hex-Notation")
  $scope.messageBlob = MessageBlob(keyFormat = "UUID", keyAuto = true)

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
  $scope.publishMessage = (blob: MessageBlob) => {
    if (validatePublishMessage(blob)) {
      for {
        topic <- blob.topic.map(_.topic)
        key <- blob.key
        message <- blob.message
        keyFormat <- blob.keyFormat
        messageFormat <- blob.messageFormat
      } {
        messageSvc.publishMessage(topic, key, message, keyFormat, messageFormat).withGlobalLoading.withTimer("Retrieving Zookeeper data") onComplete {
          case Success(response) =>
            //$scope.messageBlob.message = null
            $log.info(s"response = ${angular.toJson(response)}")
            if (response.`type`.contains("error"))
              $scope.addErrorMessage(response.message)
            else
              toaster.success("Message published")

          case Failure(e) =>
            $scope.addErrorMessage(e.displayMessage)
        }
      }
    }
  }

  /**
    * Validates the given message blob
    * @param blob the given message blob
    * @return {boolean}
    */
  private def validatePublishMessage(blob: MessageBlob) = {
    if (!blob.topic.exists(_.topic.nonBlank)) {
      $scope.addErrorMessage("No topic specified")
      false
    }
    else if (!blob.keyFormat.exists(_.nonBlank)) {
      $scope.addErrorMessage("No message key format specified")
      false
    }
    else if (!blob.message.exists(_.nonBlank)) {
      $scope.addErrorMessage("No message body specified")
      false
    }
    else if (!blob.messageFormat.exists(_.nonBlank)) {
      $scope.addErrorMessage("No message body format specified")
      false
    }
    else true
  }

}

/**
  * Publish Controller Scope
  * @author lawrence.daniels@gmail.com
  */
@js.native
trait PublishControllerScope extends Scope with GlobalErrorHandling with GlobalLoading {
  // properties
  var keyFormats: js.Array[String] = js.native
  var messageBlob: MessageBlob = js.native
  var messageFormats: js.Array[String] = js.native

  // functions
  var init: js.Function0[Unit] = js.native
  var publishMessage: js.Function1[MessageBlob, Unit] = js.native
}
