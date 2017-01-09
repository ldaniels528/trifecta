package com.github.ldaniels528.trifecta.ui.models

import play.api.libs.json.{Json, Reads, Writes}

/**
  * Consumer-By-Topic JSON model
  * @author lawrence.daniels@gmail.com
  */
case class ConsumerByTopicJs(consumerId: String, details: Seq[ConsumerDetailJs])

/**
  * Consumer-By-Topic JSON Companion Object
  * @author lawrence.daniels@gmail.com
  */
object ConsumerByTopicJs {

  implicit val ConsumerByTopicReads: Reads[ConsumerByTopicJs] = Json.reads[ConsumerByTopicJs]

  implicit val ConsumerByTopicWrites: Writes[ConsumerByTopicJs] = Json.writes[ConsumerByTopicJs]

}
