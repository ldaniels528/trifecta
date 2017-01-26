package com.github.ldaniels528.trifecta.sjs.services

import com.github.ldaniels528.trifecta.sjs.controllers.MessageSearchController.MessageSearchForm
import io.scalajs.npm.angularjs.Service
import io.scalajs.npm.angularjs.uibootstrap.{Modal, ModalOptions}

/**
  * Message Search Service
  * @author lawrence.daniels@gmail.com
  */
class MessageSearchService($modal: Modal) extends Service {

  /**
    * Message Search Finder Modal Dialog
    */
  def finderDialog() = {
    val $modalInstance = $modal.open[MessageSearchForm](new ModalOptions(
      controller = "MessageSearchController",
      templateUrl = "message_search_finder.htm"
    ))
    $modalInstance.result.toFuture
  }

  /**
    * Message Search Loading Modal Dialog
    */
  def loadingDialog() = {
    val $modalInstance = $modal.open[MessageSearchForm](new ModalOptions(
      controller = "MessageSearchController",
      templateUrl = "message_search_loading.htm"
    ))
    $modalInstance.result.toFuture
  }

}
