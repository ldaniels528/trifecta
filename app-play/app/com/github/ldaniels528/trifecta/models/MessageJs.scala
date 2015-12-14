package com.github.ldaniels528.trifecta.models

import play.api.libs.json.{JsValue, Json}

/**
  * Message JSON model
  * @author lawrence.daniels@gmail.com
  */
case class MessageJs(`type`: String,
                     payload: Option[JsValue] = None,
                     topic: Option[String] = None,
                     partition: Option[Int] = None,
                     offset: Option[Long] = None,
                     message: Option[String] = None )

/**
  * Message JSON Companion Object
  * @author lawrence.daniels@gmail.com
  */
object MessageJs {

  implicit val MessageReads = Json.reads[MessageJs]

  implicit val MessageWrites = Json.writes[MessageJs]

}
