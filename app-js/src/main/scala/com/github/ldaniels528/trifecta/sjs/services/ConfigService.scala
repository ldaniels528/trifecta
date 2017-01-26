package com.github.ldaniels528.trifecta.sjs.services

import io.scalajs.npm.angularjs.Service
import io.scalajs.npm.angularjs.http.{Http, HttpResponse}

import scala.scalajs.js

/**
  * Configuration Service
  * @author lawrence.daniels@gmail.com
  */
class ConfigService($http: Http) extends Service {

  /**
    * Retrieves the current configuration from the server
    * @return an HTTP response containing the [[js.Dictionary configuration]]
    */
  def getConfig: HttpResponse[js.Dictionary[js.Any]] = {
    $http.get[js.Dictionary[js.Any]]("/api/config")
  }

}
