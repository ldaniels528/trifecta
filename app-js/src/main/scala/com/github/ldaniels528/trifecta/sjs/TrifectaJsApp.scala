package com.github.ldaniels528.trifecta.sjs

import com.github.ldaniels528.trifecta.sjs.controllers._
import com.github.ldaniels528.trifecta.sjs.services._
import org.scalajs.angularjs.angular
import org.scalajs.angularjs.http.HttpProvider
import org.scalajs.angularjs.uirouter.{RouteProvider, RouteTo}
import org.scalajs.dom.browser.console

import scala.scalajs.js
import scala.scalajs.js.annotation.JSExport

/**
  * Trifecta Scala.Js Application
  * @author lawrence.daniels@gmail.com
  */
object TrifectaJsApp extends js.JSApp {
  val appName = "trifecta"
  val appVersion = "0.22.0"

  @JSExport
  override def main() {
    // create the application
    val module = angular.createModule(appName, js.Array(
      "ngAnimate", "ngCookies", "ngResource", "ngRoute", "ngSanitize", "hljs", "toaster", "ui.bootstrap"
    ))

    // configure the controllers
    module.controllerOf[DecoderController]("DecoderController")
    module.controllerOf[InspectController]("InspectController")
    module.controllerOf[MainController]("MainController")
    module.controllerOf[ObserveController]("ObserveController")
    module.controllerOf[PublishController]("PublishController")
    module.controllerOf[QueryController]("QueryController")

    // configure the services
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
        .when("/decoders", RouteTo(templateUrl = "/assets/views/decoders.html"))
        .when("/inspect", RouteTo(templateUrl = "/assets/views/inspect/index.html", reloadOnSearch = false))
        .when("/observe", RouteTo(templateUrl = "/assets/views/observe.html", reloadOnSearch = false, controller = classOf[ObserveController].getSimpleName))
        .when("/publish", RouteTo(templateUrl = "/assets/views/publish.html"))
        .when("/query", RouteTo(templateUrl = "/assets/views/query.html"))
        .otherwise(RouteTo(redirectTo = "/inspect/brokers"))
      ()
    })

    // start the application
    module.run({ ($rootScope: RootScope, ServerSideEventsService: ServerSideEventsService) =>
      $rootScope.version = appVersion

      console.log("Initializing application...")
      ServerSideEventsService.connect()
      ()
    })
  }

}

