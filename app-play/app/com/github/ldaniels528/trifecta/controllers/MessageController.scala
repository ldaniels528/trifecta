package com.github.ldaniels528.trifecta.controllers

import java.util.UUID

import akka.actor.Props
import akka.util.Timeout
import com.github.ldaniels528.trifecta.actors.ReactiveEventsActor
import com.github.ldaniels528.trifecta.actors.ReactiveEventsActor.SamplingSession
import com.github.ldaniels528.trifecta.controllers.MessageController._
import com.github.ldaniels528.trifecta.models.{MessageSamplingStartRequest, StreamingConsumerUpdateRequest, StreamingTopicUpdateRequest}
import play.api.libs.concurrent.Execution.Implicits._
import play.api.libs.json.Json
import play.api.mvc.{Action, Controller}
import play.libs.Akka

import scala.collection.concurrent.TrieMap
import scala.concurrent.Future
import scala.concurrent.duration._
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

  ///////////////////////////////////////////////////////////////////////////
  //    Message Sampling
  ///////////////////////////////////////////////////////////////////////////

  def getSamplingSession() = Action { implicit request =>
    request.session.get("sessionId") match {
      case Some(sessionId) => Ok(Json.obj("sessionId" -> sessionId))
      case None => BadRequest("No session found")
    }
  }

  def startSampling() = Action.async { implicit request =>
    import akka.pattern.ask

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
        } recover { case e =>
          InternalServerError(e.getMessage)
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
object MessageController {
  private val sessions = TrieMap[String, SamplingSession]()
  private val reactiveActor = Akka.system.actorOf(Props[ReactiveEventsActor])

  // schedule streaming updates
  reactiveActor ! StreamingConsumerUpdateRequest(15)
  reactiveActor ! StreamingTopicUpdateRequest(15)

}