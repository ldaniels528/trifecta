package com.github.ldaniels528.trifecta.controllers

import com.github.ldaniels528.trifecta.models.{QueryDetailsJs, QueryJs}
import play.api.libs.concurrent.Execution.Implicits._
import play.api.libs.json.{JsValue, Json}
import play.api.mvc.{Action, Controller}

import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

/**
  * Query Controller
  * @author lawrence.daniels@gmail.com
  */
class QueryController extends Controller {

  def executeQuery = Action.async { implicit request =>
    request.body.asJson.flatMap(_.asOpt[QueryJs]) match {
      case Some(query) =>
        WebConfig.facade.executeQuery(query).mapTo[JsValue] map { message =>
          Ok(Json.toJson(message))
        } recover { case e: Throwable =>
          InternalServerError(e.getMessage)
        }
      case None =>
        Future.successful(BadRequest("Query object expected"))
    }
  }

  def findOne(topic: String) = Action.async { implicit request =>
    request.body.asText match {
      case Some(criteria) =>
        WebConfig.facade.findOne(topic, criteria).mapTo[JsValue] map { message =>
          Ok(Json.toJson(message))
        } recover { case e: Throwable =>
          InternalServerError(e.getMessage)
        }
      case None =>
        Future.successful(BadRequest("Query object expected"))
    }
  }

  def getQueries = Action {
    Try(WebConfig.facade.getQueries) match {
      case Success(queries) => Ok(Json.toJson(queries))
      case Failure(e) => InternalServerError(e.getMessage)
    }
  }

  def getQueriesByTopic(topic: String) = Action {
    Try(WebConfig.facade.getQueriesByTopic(topic)) match {
      case Success(queries) => Ok(Json.toJson(queries))
      case Failure(e) => InternalServerError(e.getMessage)
    }
  }

  def saveQuery = Action.async { implicit request =>
    request.body.asJson.flatMap(_.asOpt[QueryDetailsJs]) match {
      case Some(query) =>
        WebConfig.facade.saveQuery(query) map { message =>
          Ok(Json.toJson(message))
        } recover { case e: Throwable =>
          InternalServerError(e.getMessage)
        }
      case None =>
        Future.successful(BadRequest("Query object expected"))
    }
  }

  def transformResultsToCSV = Action.async { implicit request =>
    request.body.asText match {
      case Some(results) =>
        WebConfig.facade.transformResultsToCSV(results) map {
          case Some(message) => Ok(Json.toJson(message))
          case None => NotFound("No results")
        } recover { case e: Throwable =>
          InternalServerError(e.getMessage)
        }
      case None =>
        Future.successful(BadRequest("Schema object expected"))
    }
  }

}
