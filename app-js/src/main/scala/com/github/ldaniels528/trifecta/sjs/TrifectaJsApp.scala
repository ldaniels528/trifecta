package com.github.ldaniels528.trifecta.sjs

import com.github.ldaniels528.scalascript.core.HttpProvider
import com.github.ldaniels528.scalascript.extensions.{RouteProvider, RouteTo}
import com.github.ldaniels528.scalascript.{Scope, angular}
import com.github.ldaniels528.trifecta.sjs.controllers._
import com.github.ldaniels528.trifecta.sjs.services._
import org.scalajs.dom.console

import scala.scalajs.js
import scala.scalajs.js.annotation.JSExport

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
    module.controllerOf[DecoderController]("DecoderCtrl")
    module.controllerOf[InspectController]("InspectCtrl")
    module.controllerOf[MainController]("MainCtrl")
    module.controllerOf[ObserveController]("ObserveCtrl")
    module.controllerOf[PublishController]("PublishCtrl")
    module.controllerOf[QueryController]("QueryCtrl")

    // configure the services
    module.serviceOf[DecoderService]("DecoderSvc")
    module.serviceOf[MessageDataService]("MessageSvc")
    module.serviceOf[QueryService]("QuerySvc")
    module.serviceOf[ServerSideEventsService]("ServerSideEventsSvc")
    module.serviceOf[TopicService]("TopicSvc")
    module.serviceOf[ZookeeperService]("ZookeeperSvc")

    // configure the filters
    module.filter("capitalize", Filters.capitalize)
    module.filter("duration", Filters.duration)
    module.filter("yesno", Filters.yesNo)

    // configure the dialogs
    module.controllerOf[MessageSearchController]("MessageSearchCtrl")
    module.serviceOf[MessageSearchService]("MessageSearchSvc")

    // configure the application
    module.config({ ($httpProvider: HttpProvider, $routeProvider: RouteProvider) =>
      $routeProvider
        .when("/decoders", RouteTo(templateUrl = "/assets/views/decoders.html"))
        .when("/inspect", RouteTo(templateUrl = "/assets/views/inspect/index.html", reloadOnSearch = false))
        .when("/observe", RouteTo(templateUrl = "/assets/views/observe.html"))
        .when("/publish", RouteTo(templateUrl = "/assets/views/publish.html"))
        .when("/query", RouteTo(templateUrl = "/assets/views/query.html"))
        .otherwise(RouteTo(redirectTo = "/inspect/brokers"))
      ()
    })

    // start the application
    module.run({ ($rootScope: RootScope, ServerSideEventsSvc: ServerSideEventsService) =>
      $rootScope.version = "0.19.0"

      console.log("Initializing application...")
      ServerSideEventsSvc.connect()
      ()
    })
  }

}

/**
  * Trifecta Application Root Scope
  * @author lawrence.daniels@gmail.com
  */
@js.native
trait RootScope extends Scope {
  var version: js.UndefOr[String] = js.native
}
