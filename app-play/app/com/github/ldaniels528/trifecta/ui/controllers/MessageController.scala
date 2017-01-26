package com.github.ldaniels528.trifecta.ui.controllers

import play.api.libs.concurrent.Execution.Implicits._
import play.api.libs.json.Json
import play.api.mvc.{Action, Controller}

import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

/**
  * Message Controller
  * @author lawrence.daniels@gmail.com
  */
class MessageController() extends Controller {

  def getMessageData(topic: String, partition: Int, offset: Long, decode: Boolean) = Action {
    Try(WebConfig.facade.getMessageData(topic, partition, offset, decode)) match {
      case Success(messageData) => Ok(Json.toJson(messageData))
      case Failure(e) => InternalServerError(e.getMessage)
    }
  }

  def getMessageKey(topic: String, partition: Int, offset: Long, decode: Boolean) = Action {
    Try(WebConfig.facade.getMessageKey(topic, partition, offset, decode)) match {
      case Success(messageKey) => Ok(Json.toJson(messageKey))
      case Failure(e) => InternalServerError(e.getMessage)
    }
  }

  def publishMessage(topic: String) = Action.async { implicit request =>
    request.body.asJson match {
      case Some(jsonBody) =>
        WebConfig.facade.publishMessage(topic, jsonBody.toString()) map { resp =>
          Ok(Json.obj("topic" -> resp.topic(), "partition" -> resp.partition(), "offset" -> resp.offset()))
        } recover {
          case e: Throwable =>
            InternalServerError(e.getMessage)
        }
      case None =>
        Future.successful(BadRequest("Publish message request object expected"))
    }
  }

}
