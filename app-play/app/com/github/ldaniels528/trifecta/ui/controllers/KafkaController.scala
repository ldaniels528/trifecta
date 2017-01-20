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

/**
  * Kafka Controller
  * @author lawrence.daniels@gmail.com
  */
class KafkaController() extends Controller {

  // one-time initialization
  if (initialized.compareAndSet(false, true)) {
    // push consumer offset updates to clients
    Akka.system.scheduler.schedule(initialDelay = 15.seconds, interval = WebConfig.getConsumerPushInterval) {
      val deltas = WebConfig.facade.getConsumerDeltas
      if (deltas.nonEmpty) {
        Logger.debug(s"Sending ${deltas.size} consumer offset updates...")
        SSE ! SSEMessage(`type` = CONSUMER_DELTAS, message = Json.toJson(deltas))
      }
    }

    // push topic offset updates to clients
    Akka.system.scheduler.schedule(initialDelay = 1.minute, interval = WebConfig.getTopicOffsetsPushInterval) {
      val deltas = WebConfig.facade.getTopicDeltas
      if (deltas.nonEmpty) {
        Logger.debug(s"Sending ${deltas.size} topic offset updates...")
        SSE ! SSEMessage(`type` = TOPIC_DELTAS, message = Json.toJson(deltas))
      }
    }
  }

  def getBrokers = Action.async {
    Future(WebConfig.facade.getBrokers) map { brokers =>
      Ok(Json.toJson(brokers))
    } recover { case e: Throwable =>
      e.printStackTrace()
      InternalServerError(e.getMessage)
    }
  }

  def getBrokerDetails = Action.async {
    Future(WebConfig.facade.getBrokerDetails) map { details =>
      Ok(Json.toJson(details))
    } recover { case e: Throwable =>
      e.printStackTrace()
      InternalServerError(e.getMessage)
    }
  }

  def getConsumerDeltas = Action.async {
    Future(WebConfig.facade.getConsumerDeltas) map { deltas =>
      Ok(Json.toJson(deltas))
    } recover { case e: Throwable =>
      e.printStackTrace()
      InternalServerError(e.getMessage)
    }
  }

  def getConsumerGroup(groupId: String) = Action.async {
    Future(WebConfig.facade.getConsumerGroup(groupId)) map {
      case Some(group) => Ok(Json.toJson(group))
      case None => NotFound(groupId)
    } recover { case e: Throwable =>
      e.printStackTrace()
      InternalServerError(e.getMessage)
    }
  }

  def getConsumerOffsets(groupId: String) = Action.async {
    Future(WebConfig.facade.getConsumerOffsets(groupId)) map { offsets =>
      Ok(Json.toJson(offsets))
    } recover { case e: Throwable =>
      e.printStackTrace()
      InternalServerError(e.getMessage)
    }
  }

  def getConsumersLite = Action.async {
    Future(WebConfig.facade.getConsumerSkeletons) map { skeletons =>
      Ok(Json.toJson(skeletons))
    } recover { case e: Throwable =>
      e.printStackTrace()
      InternalServerError(e.getMessage)
    }
  }

  def getReplicas(topic: String) = Action.async {
    Future(WebConfig.facade.getReplicas(topic)) map { replicas =>
      Ok(Json.toJson(replicas))
    } recover { case e: Throwable =>
      e.printStackTrace()
      InternalServerError(e.getMessage)
    }
  }

  def getTopicByName(topic: String) = Action.async {
    Future(WebConfig.facade.getTopicByName(topic)) map {
      case Some(details) => Ok(Json.toJson(details))
      case None => NotFound(topic)
    } recover { case e: Throwable =>
      e.printStackTrace()
      InternalServerError(e.getMessage)
    }
  }

  def getTopicDeltas = Action.async {
    Future(WebConfig.facade.getTopicDeltas) map { deltas =>
      Ok(Json.toJson(deltas))
    } recover { case e: Throwable =>
      e.printStackTrace()
      InternalServerError(e.getMessage)
    }
  }

  def getTopicDetailsByName(topic: String) = Action.async {
    Future(WebConfig.facade.getTopicDetailsByName(topic)) map { deltas =>
      Ok(Json.toJson(deltas))
    } recover { case e: Throwable =>
      e.printStackTrace()
      InternalServerError(e.getMessage)
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