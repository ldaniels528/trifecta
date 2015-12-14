package com.github.ldaniels528.trifecta.models

import play.api.libs.json.Json

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

  implicit val ConsumerByTopicReads = Json.reads[ConsumerByTopicJs]

  implicit val ConsumerByTopicWrites = Json.writes[ConsumerByTopicJs]

}
