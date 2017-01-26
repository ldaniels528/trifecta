package com.github.ldaniels528.trifecta.ui.controllers

import java.util.UUID

import akka.actor.Props
import akka.util.Timeout
import com.github.ldaniels528.trifecta.ui.actors.ReactiveEventsActor
import com.github.ldaniels528.trifecta.ui.actors.ReactiveEventsActor.SamplingSession
import com.github.ldaniels528.trifecta.ui.controllers.StreamingController._
import com.github.ldaniels528.trifecta.ui.models.MessageSamplingStartRequest
import play.api.libs.concurrent.Execution.Implicits._
import play.api.libs.json.Json
import play.api.mvc.{Action, Controller}
import play.libs.Akka

import scala.collection.concurrent.TrieMap
import scala.concurrent.Future
import scala.concurrent.duration._

/**
  * Streaming Controller
  * @author lawrence.daniels@gmail.com
  */
class StreamingController() extends Controller {

  def getSamplingSession = Action { implicit request =>
    request.session.get("sessionId") match {
      case Some(sessionId) => Ok(Json.obj("sessionId" -> sessionId))
      case None => BadRequest("No session found")
    }
  }

  def startSampling = Action.async { implicit request =>
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
  * Streaming Controller Companion
  * @author lawrence.daniels@gmail.com
  */
object StreamingController {
  private val sessions = TrieMap[String, SamplingSession]()
  private val reactiveActor = Akka.system.actorOf(Props[ReactiveEventsActor])

}