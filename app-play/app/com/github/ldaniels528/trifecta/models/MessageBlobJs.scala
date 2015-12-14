package com.github.ldaniels528.trifecta.models

import play.api.libs.json.Json

/**
  * Message Blob JSON model
  * @author lawrence.daniels@gmail.com
  */
case class MessageBlobJs(key: Option[String], message: String, keyFormat: String, messageFormat: String)

/**
  * Message Blob JSON Companion Object
  * @author lawrence.daniels@gmail.com
  */
object MessageBlobJs {

  implicit val MessageBlobReads = Json.reads[MessageBlobJs]

  implicit val MessageBlobWrites = Json.writes[MessageBlobJs]

}
