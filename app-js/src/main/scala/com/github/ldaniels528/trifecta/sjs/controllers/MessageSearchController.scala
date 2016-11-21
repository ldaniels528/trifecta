package com.github.ldaniels528.trifecta.sjs.controllers

import com.github.ldaniels528.trifecta.sjs.controllers.MessageSearchController._
import com.github.ldaniels528.trifecta.sjs.models.TopicDetails
import com.github.ldaniels528.trifecta.sjs.services.TopicService
import org.scalajs.angularjs.AngularJsHelper._
import org.scalajs.angularjs.toaster.Toaster
import org.scalajs.angularjs.uibootstrap.ModalInstance
import org.scalajs.angularjs.{Controller, Scope, injected}
import org.scalajs.dom.browser.console
import org.scalajs.nodejs.util.ScalaJsHelper._

import scala.scalajs.concurrent.JSExecutionContext.Implicits.queue
import scala.scalajs.js
import scala.scalajs.js.annotation.ScalaJSDefined
import scala.util.{Failure, Success}

/**
  * Message Search Controller
  * @author lawrence.daniels@gmail.com
  */
case class MessageSearchController($scope: MessageSearchScope, $modalInstance: ModalInstance[MessageSearchForm],
                                   toaster: Toaster,
                                   @injected("TopicService") topicService: TopicService)
  extends Controller with PopupMessages {

  implicit val scope: Scope with GlobalLoading = $scope

  $scope.form = new MessageSearchForm()
  $scope.topics = emptyArray

  $scope.getTopics = (hideEmptyTopics: js.UndefOr[Boolean]) => $scope.topics

  $scope.ok = () => $modalInstance.close($scope.form)

  $scope.cancel = () => $modalInstance.dismiss("cancel")

  // load the topics
  topicService.getDetailedTopics /*.withGlobalLoading*/ onComplete {
    case Success(loadedTopics) =>
      if (loadedTopics.nonEmpty) console.warn("No topic summaries found")
      $scope.$apply(() => $scope.topics = loadedTopics)
    case Failure(e) =>
      errorPopup(e.displayMessage, e)
  }

}

/**
  * Message Search Controller Companion
  * @author lawrence.daniels@gmail.com
  */
object MessageSearchController {

  /**
    * Message Search Form
    * @author lawrence.daniels@gmail.com
    */
  @ScalaJSDefined
  class MessageSearchForm(var topic: js.UndefOr[TopicDetails] = js.undefined,
                          var criteria: js.UndefOr[String] = js.undefined) extends js.Object

  /**
    * Message Search Controller Scope
    * @author lawrence.daniels@gmail.com
    */
  @js.native
  trait MessageSearchScope extends Scope with GlobalLoading {
    // properties
    var form: MessageSearchForm = js.native
    var topics: js.Array[TopicDetails] = js.native

    // functions
    var ok: js.Function0[Unit] = js.native
    var cancel: js.Function0[Unit] = js.native
    var getTopics: js.Function1[js.UndefOr[Boolean], js.Array[TopicDetails]] = js.native
  }

}