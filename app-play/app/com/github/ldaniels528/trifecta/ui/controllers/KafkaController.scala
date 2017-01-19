package com.github.ldaniels528.trifecta.ui.controllers

import java.util.concurrent.atomic.AtomicBoolean

import com.github.ldaniels528.trifecta.AppConstants._
import com.github.ldaniels528.trifecta.ui.actors.{SSE, SSEMessage}
import com.github.ldaniels528.trifecta.ui.controllers.KafkaController._
import com.github.ldaniels528.trifecta.ui.models.ConsumerGroupJs._
import play.api.Logger
import play.api.libs.concurrent.Execution.Implicits._
import play.api.libs.json.Json
import play.api.mvc.{Action, Controller}
import play.libs.Akka

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

/**
  * Kafka Controller
  * @author lawrence.daniels@gmail.com
  */
class KafkaController() extends Controller {

  // one-time initialization
  if(initialized.compareAndSet(false, true)) {
    // push topic offset updates to clients
    Akka.system.scheduler.schedule(initialDelay = 15.seconds, interval = WebConfig.getTopicOffsetsPushInterval) {
      val deltas = WebConfig.facade.getConsumerDeltas
      if (deltas.nonEmpty) {
        Logger.debug(s"Sending ${deltas.size} consumer offset updates...")
        SSE ! SSEMessage(`type` = CONSUMER_DELTAS, message = Json.toJson(deltas))
      }
    }

    // push consumer offset updates to clients
    Akka.system.scheduler.schedule(initialDelay = 1.minute, interval = WebConfig.getConsumerPushInterval) {
      val deltas = WebConfig.facade.getTopicDeltas
      if (deltas.nonEmpty) {
        Logger.debug(s"Sending ${deltas.size} topic offset updates...")
        SSE ! SSEMessage(`type` = TOPIC_DELTAS, message = Json.toJson(deltas))
      }
    }
  }

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

  def getConsumerDeltas = Action {
    Try(WebConfig.facade.getConsumerDeltas) match {
      case Success(deltas) => Ok(Json.toJson(deltas))
      case Failure(e) => InternalServerError(e.getMessage)
    }
  }

  def getConsumerGroup(groupId: String) = Action {
    Try(WebConfig.facade.getConsumerGroup(groupId)) match {
      case Success(Some(group)) => Ok(Json.toJson(group))
      case Success(None) => NotFound(groupId)
      case Failure(e) =>
        Logger.error("Internal server error", e)
        InternalServerError(e.getMessage)
    }
  }

  def getConsumerOffsets(groupId: String) = Action {
    Try(WebConfig.facade.getConsumerOffsets(groupId)) match {
      case Success(offsets) => Ok(Json.toJson(offsets))
      case Failure(e) =>
        Logger.error("Internal server error", e)
        InternalServerError(e.getMessage)
    }
  }

  def getConsumersLite = Action {
    Try(WebConfig.facade.getConsumerSkeletons) match {
      case Success(skeletons) => Ok(Json.toJson(skeletons))
      case Failure(e) =>
        Logger.error("Internal server error", e)
        InternalServerError(e.getMessage)
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
    Future(WebConfig.facade.getTopicSummaries) map { summaries =>
      Ok(Json.toJson(summaries))
    } recover { case e: Throwable =>
      e.printStackTrace()
      InternalServerError(e.getMessage)
    }
  }

  def getTopics = Action.async {
    Future(WebConfig.facade.getTopics) map { details =>
      Ok(Json.toJson(details))
    } recover { case e: Throwable =>
      InternalServerError(e.getMessage)
    }
  }

}

/**
  * KafkaController Companion
  * @author lawrence.daniels@gmail.com
  */
object KafkaController {
  private val initialized = new AtomicBoolean(false)

}