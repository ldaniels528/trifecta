package com.github.ldaniels528.trifecta.actors

import play.api.libs.json.{JsValue, Json}

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

  implicit val SSEReads = Json.reads[SSEMessage]

  implicit val SSEWrites = Json.writes[SSEMessage]

}