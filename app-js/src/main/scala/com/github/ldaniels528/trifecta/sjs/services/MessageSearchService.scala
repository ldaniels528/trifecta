package com.github.ldaniels528.trifecta.sjs.services

import com.github.ldaniels528.scalascript.Service
import com.github.ldaniels528.scalascript.extensions.{Modal, ModalOptions}
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
    val $modalInstance = $modal.open[MessageSearchForm](ModalOptions(
      controller = "MessageSearchCtrl",
      templateUrl = "message_search_finder.htm"
    ))
    $modalInstance.result
  }

  /**
    * Message Search Loading Modal Dialog
    */
  def loadingDialog() = {
    val $modalInstance = $modal.open[MessageSearchForm](ModalOptions(
      controller = "MessageSearchCtrl",
      templateUrl = "message_search_loading.htm"
    ))
    $modalInstance.result
  }

}
