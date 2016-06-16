package com.github.ldaniels528.trifecta.sjs.services

import org.scalajs.angularjs.Service
import org.scalajs.angularjs.uibootstrap.{Modal, ModalOptions}
import org.scalajs.dom.browser.console
import com.github.ldaniels528.trifecta.sjs.controllers.MessageSearchForm

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
      controller = "MessageSearchCtrl",
      templateUrl = "message_search_finder.htm"
    ))
    $modalInstance.result.toFuture
  }

  /**
    * Message Search Loading Modal Dialog
    */
  def loadingDialog() = {
    val $modalInstance = $modal.open[MessageSearchForm](new ModalOptions(
      controller = "MessageSearchCtrl",
      templateUrl = "message_search_loading.htm"
    ))
    $modalInstance.result.toFuture
  }

}
