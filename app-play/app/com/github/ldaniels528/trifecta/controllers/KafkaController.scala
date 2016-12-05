package com.github.ldaniels528.trifecta.controllers

import play.api.Logger
import play.api.libs.concurrent.Execution.Implicits._
import play.api.libs.json.Json
import play.api.mvc.{Action, Controller}

import scala.util.{Failure, Success, Try}

/**
  * Kafka Controller
  * @author lawrence.daniels@gmail.com
  */
class KafkaController() extends Controller {

  def getBrokers = Action {
    Try(WebConfig.facade.getBrokers) match {
      case Success(brokers) => Ok(Json.toJson(brokers))
      case Failure(e) => InternalServerError(e.getMessage)
    }
  }

  def getBrokerDetails = Action {
    Try(WebConfig.facade.getBrokerDetails) match {
      case Success(details) => Ok(Json.toJson(details))
      case Failure(e) =>
        e.printStackTrace()
        InternalServerError(e.getMessage)
    }
  }

  def getConsumers = Action {
    Try(WebConfig.facade.getConsumersGroupedByID) match {
      case Success(details) => Ok(Json.toJson(details))
      case Failure(e) =>
        Logger.error("Internal server error", e)
        InternalServerError(e.getMessage)
    }
  }

  def getConsumerDeltas = Action {
    Try(WebConfig.facade.getConsumerDeltas) match {
      case Success(deltas) => Ok(Json.toJson(deltas))
      case Failure(e) => InternalServerError(e.getMessage)
    }
  }

  def getConsumerDetails = Action {
    Try(WebConfig.facade.getConsumerDetails) match {
      case Success(details) => Ok(Json.toJson(details))
      case Failure(e) =>
        Logger.error("Internal server error", e)
        InternalServerError(e.getMessage)
    }
  }

  def getConsumersByTopic(topic: String) = Action {
    Try(WebConfig.facade.getConsumersByTopic(topic)) match {
      case Success(details) => Ok(Json.toJson(details))
      case Failure(e) => InternalServerError(e.getMessage)
    }
  }

  def getReplicas(topic: String) = Action {
    Try(WebConfig.facade.getReplicas(topic)) match {
      case Success(replicas) => Ok(Json.toJson(replicas))
      case Failure(e) => InternalServerError(e.getMessage)
    }
  }

  def getTopicByName(topic: String) = Action {
    Try(WebConfig.facade.getTopicByName(topic)) match {
      case Success(Some(details)) => Ok(Json.toJson(details))
      case Success(None) => NotFound(topic)
      case Failure(e) => InternalServerError(e.getMessage)
    }
  }

  def getTopicDeltas = Action {
    Try(WebConfig.facade.getTopicDeltas) match {
      case Success(deltas) => Ok(Json.toJson(deltas))
      case Failure(e) =>
        e.printStackTrace()
        InternalServerError(e.getMessage)
    }
  }

  def getTopicDetailsByName(topic: String) = Action {
    Try(WebConfig.facade.getTopicDetailsByName(topic)) match {
      case Success(deltas) => Ok(Json.toJson(deltas))
      case Failure(e) => InternalServerError(e.getMessage)
    }
  }

  def getTopicSummaries = Action.async {
    WebConfig.facade.getTopicSummaries map { summaries =>
      Ok(Json.toJson(summaries))
    } recover { case e: Throwable =>
      e.printStackTrace()
      InternalServerError(e.getMessage)
    }
  }

  def getTopics = Action.async {
    WebConfig.facade.getTopics map { details =>
      Ok(Json.toJson(details))
    } recover { case e: Throwable =>
      InternalServerError(e.getMessage)
    }
  }

}