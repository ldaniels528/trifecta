package com.github.ldaniels528.trifecta.sjs.controllers

import com.github.ldaniels528.trifecta.sjs.services.ConfigService
import io.scalajs.npm.angularjs._
import io.scalajs.npm.angularjs.toaster.Toaster

import scala.concurrent.duration._
import scala.scalajs.concurrent.JSExecutionContext.Implicits.queue
import scala.scalajs.js
import scala.util.{Failure, Success}

/**
  * Configuration Controller
  * @author lawrence.daniels@gmail.com
  */
case class ConfigController($scope: ConfigScope, $log: Log, $timeout: Timeout, toaster: Toaster,
                            @injected("ConfigService") configService: ConfigService)
  extends Controller with PopupMessages {

  private val knownConfigs = js.Array(
    "trifecta.common.encoding",
    "trifecta.kafka.consumers.native",
    "trifecta.kafka.consumers.zookeeper",
    "trifecta.web.push.interval.consumer",
    "trifecta.web.push.interval.topic",
    "trifecta.zookeeper.host",
    "trifecta.zookeeper.kafka.root.path"
  )

  $scope.init = () => {
    $log.info(s"Initializing ${getClass.getName}...")
    loadConfigurationProperties()
  }

  $scope.isConfigKey = (aLabel: js.UndefOr[String]) => {
    aLabel.exists(s => knownConfigs.contains(s.trim.toLowerCase))
  }

  private def loadConfigurationProperties() = {
    $scope.loadingConfig = true
    configService.getConfig onComplete {
      case Success(props) =>
        $timeout(() => $scope.loadingConfig = false, 500.millis)
        $log.info(s"Loading ${props.size} configuration properties")
        $scope.$apply(() => $scope.configProps = props)
      case Failure(e) =>
        $scope.$apply(() => $scope.loadingConfig = false)
        errorPopup("Error loading configuration details", e)
    }
  }

}

@js.native
trait ConfigScope extends Scope {
  // variables
  var configProps: js.UndefOr[js.Dictionary[js.Any]] = js.native
  var loadingConfig: js.UndefOr[Boolean] = js.native

  // functions
  var init: js.Function0[Unit] = js.native
  var isConfigKey: js.Function1[js.UndefOr[String], Boolean] = js.native
}