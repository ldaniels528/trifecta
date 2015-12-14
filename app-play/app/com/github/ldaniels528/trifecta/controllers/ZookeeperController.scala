package com.github.ldaniels528.trifecta.controllers

import play.api.libs.concurrent.Execution.Implicits._
import play.api.libs.json.Json
import play.api.mvc.{Action, Controller}

import scala.util.{Failure, Success, Try}

/**
  * Zookeeper Controller
  * @author lawrence.daniels@gmail.com
  */
class ZookeeperController extends Controller {

  def getZkData(path: String, format: String) = Action {
    Try(WebConfig.facade.getZkData(path, format)) match {
      case Success(Some(data)) => Ok(Json.toJson(data))
      case Success(None) => Ok(Json.obj())
      case Failure(e) => InternalServerError(e.getMessage)
    }
  }

  def getZkInfo(path: String) = Action {
    Try(WebConfig.facade.getZkInfo(path)) match {
      case Success(info) => Ok(Json.toJson(info))
      case Failure(e) => InternalServerError(e.getMessage)
    }
  }

  def getZkPath(path: String) = Action {
    Try(WebConfig.facade.getZkPath(path)) match {
      case Success(items) => Ok(Json.toJson(items))
      case Failure(e) => InternalServerError(e.getMessage)
    }
  }

}
