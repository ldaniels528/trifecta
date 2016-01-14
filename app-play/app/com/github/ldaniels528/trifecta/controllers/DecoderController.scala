package com.github.ldaniels528.trifecta.controllers

import com.github.ldaniels528.trifecta.models.SchemaJs
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

  def saveDecoderSchema = Action { implicit request =>
    request.body.asJson.flatMap(_.asOpt[SchemaJs]) match {
      case Some(schema) =>
        Try(WebConfig.facade.saveDecoderSchema(schema)) match {
          case Success(message) => Ok(Json.toJson(message))
          case Failure(e) => InternalServerError(e.getMessage)
        }
      case None =>
        BadRequest("Schema object expected")
    }
  }

}
