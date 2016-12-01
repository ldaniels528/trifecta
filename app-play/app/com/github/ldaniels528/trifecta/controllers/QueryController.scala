package com.github.ldaniels528.trifecta.controllers

import com.github.ldaniels528.trifecta.models.{QueryRequestJs, QueryResultSetJs}
import com.github.ldaniels528.trifecta.util.QueryJsonUtils._
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
    request.body.asJson.flatMap(_.asOpt[QueryRequestJs]) match {
      case Some(query) =>
        WebConfig.facade.executeQuery(query) map { kqlResult =>
          Ok(Json.toJson(new QueryResultSetJs(topic = kqlResult.topic, columns = kqlResult.labels, rows = kqlResult.values.map(_.toJson))))
        } recover { case e: Throwable =>
          e.printStackTrace()
          InternalServerError(e.getMessage)
        }
      case None =>
        Future.successful(BadRequest("Query(name, queryString) object expected in body"))
    }
  }

  def findOne(topic: String) = Action.async { implicit request =>
    request.body.asText match {
      case Some(criteria) =>
        WebConfig.facade.findOne(topic, criteria).mapTo[JsValue] map { message =>
          Ok(Json.toJson(message))
        } recover { case e: Throwable =>
          e.printStackTrace()
          InternalServerError(e.getMessage)
        }
      case None =>
        Future.successful(BadRequest("Query string expected in body"))
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

}
