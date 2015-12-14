package com.github.ldaniels528.trifecta.sjs.controllers

import com.github.ldaniels528.scalascript.extensions.ModalInstance
import com.github.ldaniels528.scalascript.util.ScalaJsHelper._
import com.github.ldaniels528.scalascript.{Controller, Scope, injected}
import com.github.ldaniels528.trifecta.sjs.controllers.GlobalLoading._
import com.github.ldaniels528.trifecta.sjs.models.TopicDetails
import com.github.ldaniels528.trifecta.sjs.services.TopicService
import org.scalajs.dom.console

import scala.scalajs.concurrent.JSExecutionContext.Implicits.runNow
import scala.scalajs.js
import scala.util.{Failure, Success}

/**
  * Message Search Controller
  * @author lawrence.daniels@gmail.com
  */
class MessageSearchController($scope: MessageSearchControllerScope, $modalInstance: ModalInstance[MessageSearchForm],
                              @injected("TopicSvc") topicSvc: TopicService)
  extends Controller {

  implicit val scope = $scope

  $scope.form = MessageSearchForm()
  $scope.topics = emptyArray

  $scope.getTopics = (hideEmptyTopics: js.UndefOr[Boolean]) => $scope.topics

  $scope.ok = () => $modalInstance.close($scope.form)

  $scope.cancel = () => $modalInstance.dismiss("cancel")

  // load the topics
  topicSvc.getDetailedTopics.withGlobalLoading onComplete {
    case Success(loadedTopics) =>
      $scope.topics = loadedTopics
      if (loadedTopics.nonEmpty) console.warn("No topic summaries found")
    case Failure(e) =>
      console.error(e.displayMessage)
  }

}

/**
  * Message Search Controller Scope
  * @author lawrence.daniels@gmail.com
  */
@js.native
trait MessageSearchControllerScope extends Scope with GlobalLoading {
  // properties
  var form: MessageSearchForm = js.native
  var topics: js.Array[TopicDetails] = js.native

  // functions
  var ok: js.Function0[Unit] = js.native
  var cancel: js.Function0[Unit] = js.native
  var getTopics: js.Function1[js.UndefOr[Boolean], js.Array[TopicDetails]] = js.native
}

/**
  * Message Search Form
  * @author lawrence.daniels@gmail.com
  */
@js.native
trait MessageSearchForm extends js.Object {
  var topic: js.UndefOr[TopicDetails] = js.native
  var criteria: js.UndefOr[String] = js.native
}

/**
  * Message Search Form Companion Object
  * @author lawrence.daniels@gmail.com
  */
object MessageSearchForm {
  def apply() = {
    val form = makeNew[MessageSearchForm]
    form
  }
}