package com.github.ldaniels528.trifecta.controllers

import java.util.UUID

import com.github.ldaniels528.trifecta.actors.SSE
import play.api.libs.EventSource
import play.api.libs.iteratee.Concurrent
import play.api.libs.json.JsValue
import play.api.mvc.{Action, Controller}

/**
  * Trifecta Application Controller
  * @author lawrence.daniels@gmail.com
  */
object Application extends Controller {

  def index = Action {
    Ok(assets.views.html.index())
  }

  def sseConnect = Action { implicit request =>
    val sessionID = UUID.randomUUID().toString.replaceAllLiterally("-", "")
    val (out, sseOutChannel) = Concurrent.broadcast[JsValue]
    SSE.connect(sessionID, sseOutChannel)
    Ok.chunked(out &> EventSource())
      .as("text/event-stream")
      .withSession(request.session + "SESSION_ID" -> sessionID)
  }

}
