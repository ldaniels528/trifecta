package com.github.ldaniels528.trifecta.ui.controllers

import play.api.libs.json.Json
import play.api.mvc.{Action, Controller}

import scala.util.{Failure, Success, Try}

/**
  * Decoder Controller
  * @author lawrence.daniels@gmail.com
  */
class DecoderController extends Controller {

  def getDecoderByTopic(topic: String) = Action {
    Try(WebConfig.facade.getDecoderByTopic(topic)) match {
      case Success(decoder) => Ok(Json.toJson(decoder))
      case Failure(e) => InternalServerError(e.getMessage)
    }
  }

  def getDecoders = Action {
    Try(WebConfig.facade.getDecoders) match {
      case Success(decoder) => Ok(Json.toJson(decoder))
      case Failure(e) => InternalServerError(e.getMessage)
    }
  }

  def getDecoderSchemaByName(topic: String, schemaName: String) = Action {
    Try(WebConfig.facade.getDecoderSchemaByName(topic, schemaName)) match {
      case Success(decoder) => Ok(Json.toJson(decoder))
      case Failure(e) => InternalServerError(e.getMessage)
    }
  }

}
