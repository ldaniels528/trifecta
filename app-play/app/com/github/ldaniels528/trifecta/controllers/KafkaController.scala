package com.github.ldaniels528.trifecta.controllers

import java.util.UUID
import java.util.concurrent.atomic.AtomicBoolean

import akka.actor.Props
import akka.pattern.ask
import akka.util.Timeout
import com.github.ldaniels528.trifecta.actors.ReactiveEventsActor
import com.github.ldaniels528.trifecta.actors.ReactiveEventsActor.SamplingSession
import com.github.ldaniels528.trifecta.controllers.KafkaController.{initialized, sessions}
import com.github.ldaniels528.trifecta.models._
import play.api.libs.concurrent.Execution.Implicits._
import play.api.libs.json.Json
import play.api.mvc.{Action, Controller}
import play.libs.Akka

import scala.collection.concurrent.TrieMap
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

/**
  * Kafka Controller
  * @author lawrence.daniels@gmail.com
  */
class KafkaController() extends Controller {
  private val system = Akka.system
  private val reactiveActor = system.actorOf(Props[ReactiveEventsActor])

  // schedule streaming updates
  if (initialized.compareAndSet(false, true)) {
    reactiveActor ! StreamingConsumerUpdateRequest(15)
    reactiveActor ! StreamingTopicUpdateRequest(15)
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
      case Failure(e) => InternalServerError(e.getMessage)
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
      case Failure(e) => InternalServerError(e.getMessage)
    }
  }

  def getConsumersByTopic(topic: String) = Action {
    Try(WebConfig.facade.getConsumersByTopic(topic)) match {
      case Success(details) => Ok(Json.toJson(details))
      case Failure(e) => InternalServerError(e.getMessage)
    }
  }

  def getMessageData(topic: String, partition: Int, offset: Long) = Action {
    Try(WebConfig.facade.getMessageData(topic, partition, offset)) match {
      case Success(messageData) => Ok(Json.toJson(messageData))
      case Failure(e) => InternalServerError(e.getMessage)
    }
  }

  def getMessageKey(topic: String, partition: Int, offset: Long) = Action {
    Try(WebConfig.facade.getMessageKey(topic, partition, offset)) match {
      case Success(messageKey) => Ok(Json.toJson(messageKey))
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
      case Failure(e) => InternalServerError(e.getMessage)
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

  def publishMessage(topic: String) = Action { implicit request =>
    request.body.asJson match {
      case Some(jsonBody) =>
        Try(WebConfig.facade.publishMessage(topic, jsonBody.toString())) match {
          case Success(response) => Ok(Json.obj("success" -> true))
          case Failure(e) => InternalServerError(e.getMessage)
        }
      case None =>
        BadRequest("Message object expected")
    }
  }

  ///////////////////////////////////////////////////////////////////////////
  //    Message Sampling
  ///////////////////////////////////////////////////////////////////////////

  def getSamplingSession = Action { implicit request =>
    request.session.get("sessionId") match {
      case Some(sessionId) => Ok(Json.obj("sessionId" -> sessionId))
      case None => BadRequest("No session found")
    }
  }

  def startSampling = Action.async { implicit request =>
    val results = for {
      startRequest <- request.body.asJson.flatMap(_.asOpt[MessageSamplingStartRequest])
      sessionId = UUID.randomUUID().toString.replaceAllLiterally("-", "")
    } yield (sessionId, startRequest)

    results match {
      case Some((sessionId, startRequest)) =>
        implicit val timeout: Timeout = 40.seconds
        val outcome = (reactiveActor ? startRequest).mapTo[SamplingSession]
        outcome map { session =>
          sessions.put(sessionId, session)
          Ok(Json.obj("sessionId" -> sessionId)).withSession("sessionId" -> sessionId)
        }
      case None =>
        Future.successful(BadRequest("Message Sampling Start Request object expected"))
    }
  }

  def stopSampling(sessionId: String) = Action { implicit request =>
    sessions.remove(sessionId) match {
      case Some(session) =>
        Ok(Json.obj("success" -> session.promise.cancel()))
      case None =>
        NotFound(s"Session ID $sessionId not found")
    }
  }

}

/**
  * Kafka Controller Companion Object
  * @author lawrence.daniels@gmail.com
  */
object KafkaController {
  val initialized = new AtomicBoolean(false)
  val sessions = TrieMap[String, SamplingSession]()

}