package com.github.ldaniels528.trifecta.ui.actors

import play.api.libs.json.{JsValue, Json, Reads, Writes}

/**
  * Represents a Server-side Event Message
  * @author lawrence.daniels@gmail.com
  */
case class SSEMessage(`type`: String, message: JsValue)

/**
  * WebSocket Event Message Companion
  * @author lawrence.daniels@gmail.com
  */
object SSEMessage {

  implicit val SSEReads: Reads[SSEMessage] = Json.reads[SSEMessage]

  implicit val SSEWrites: Writes[SSEMessage] = Json.writes[SSEMessage]

}