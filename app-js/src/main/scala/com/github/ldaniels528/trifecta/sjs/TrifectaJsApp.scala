package com.github.ldaniels528.trifecta.sjs

import scalajs.concurrent.JSExecutionContext.Implicits.queue
import scala.scalajs.js.JSConverters._
import io.scalajs.npm.angularjs.AngularJsHelper._
import com.github.ldaniels528.trifecta.AppConstants._
import com.github.ldaniels528.trifecta.sjs.controllers._
import com.github.ldaniels528.trifecta.sjs.services._
import io.scalajs.npm.angularjs.angular
import io.scalajs.npm.angularjs.http.HttpProvider
import io.scalajs.npm.angularjs.uirouter.{RouteProvider, RouteTo}
import io.scalajs.dom.html.browser.console

import scala.scalajs.js
import scala.scalajs.js.annotation.JSExport
import scala.util.{Failure, Success}

/**
  * Trifecta Scala.Js Application
  * @author lawrence.daniels@gmail.com
  */
object TrifectaJsApp extends js.JSApp {
  val appName = "trifecta"

  @JSExport
  override def main() {
    // create the application
    val module = angular.createModule(appName, js.Array(
      "ngAnimate", "ngCookies", "ngResource", "ngRoute", "ngSanitize", "hljs", "toaster", "ui.bootstrap"
    ))

    // configure the controllers
    module.controllerOf[ConfigController]("ConfigController")
    module.controllerOf[DecoderController]("DecoderController")
    module.controllerOf[InspectController]("InspectController")
    module.controllerOf[MainController]("MainController")
    module.controllerOf[ObserveController]("ObserveController")
    module.controllerOf[PublishController]("PublishController")
    module.controllerOf[QueryController]("QueryController")

    // configure the services
    module.serviceOf[BrokerService]("BrokerService")
    module.serviceOf[ConfigService]("ConfigService")
    module.serviceOf[ConsumerGroupService]("ConsumerGroupService")
    module.serviceOf[DecoderService]("DecoderService")
    module.serviceOf[MessageDataService]("MessageDataService")
    module.serviceOf[QueryService]("QueryService")
    module.serviceOf[ServerSideEventsService]("ServerSideEventsService")
    module.serviceOf[TopicService]("TopicService")
    module.serviceOf[ZookeeperService]("ZookeeperService")

    // configure the filters
    module.filter("capitalize", Filters.capitalize)
    module.filter("duration", Filters.duration)
    module.filter("yesno", Filters.yesNo)

    // configure the dialogs
    module.controllerOf[MessageSearchController]("MessageSearchController")
    module.serviceOf[MessageSearchService]("MessageSearchService")

    // configure the application
    module.config({ ($httpProvider: HttpProvider, $routeProvider: RouteProvider) =>
      $routeProvider
        .when("/decoders", new RouteTo(templateUrl = "/assets/views/decoders.html"))
        .when("/inspect", new RouteTo(templateUrl = "/assets/views/inspect/index.html", reloadOnSearch = false))
        .when("/observe", new RouteTo(templateUrl = "/assets/views/observe.html", reloadOnSearch = false, controller = classOf[ObserveController].getSimpleName))
        .when("/publish", new RouteTo(templateUrl = "/assets/views/publish.html"))
        .when("/query", new RouteTo(templateUrl = "/assets/views/query.html"))
        .otherwise(new RouteTo(redirectTo = "/inspect/brokers"))
      ()
    })

    // start the application
    module.run({ ($rootScope: RootScope,
                  ConfigService: ConfigService,
                  ServerSideEventsService: ServerSideEventsService) =>
      $rootScope.version = VERSION
      $rootScope.kafkaVersion = KAFKA_VERSION

      ConfigService.getConfig onComplete {
        case Success(props) =>
          $rootScope.zookeeper = props.get("trifecta.zookeeper.host").map(_.toString).orUndefined
        case Failure(e) =>
          console.error(s"Failed to retrieve configuration properties: ${e.displayMessage}")
      }

      console.log("Initializing application...")
      ServerSideEventsService.connect()
      ()
    })
  }

}

