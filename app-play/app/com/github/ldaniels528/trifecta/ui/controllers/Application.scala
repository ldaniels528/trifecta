package com.github.ldaniels528.trifecta.ui.controllers

import java.util.UUID

import com.github.ldaniels528.trifecta.ui.actors.SSE
import play.api.libs.EventSource
import play.api.libs.iteratee.Concurrent
import play.api.libs.json.{JsValue, Json}
import play.api.mvc.{Action, Controller}

import scala.collection.JavaConversions._

/**
  * Trifecta Application Controller
  * @author lawrence.daniels@gmail.com
  */
object Application extends Controller {
  private val SESSION_KEY = "SESSION_ID"

  def index = Action {
    Ok(assets.views.html.index())
  }

  def getConfig = Action {
    Ok(Json.toJson(WebConfig.facade.config.configProps.toMap))
  }

  def sseConnect = Action { implicit request =>
    val sessionID = request.session.get(SESSION_KEY) getOrElse UUID.randomUUID().toString.replaceAllLiterally("-", "")
    val (out, sseOutChannel) = Concurrent.broadcast[JsValue]
    SSE.connect(sessionID, sseOutChannel)
    Ok.chunked(out &> EventSource())
      .as("text/event-stream")
      .withSession(request.session + SESSION_KEY -> sessionID)
  }

}
