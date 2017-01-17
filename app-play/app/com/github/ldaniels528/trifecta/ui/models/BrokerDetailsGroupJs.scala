package com.github.ldaniels528.trifecta.ui.models

import play.api.libs.json.Json

/**
  * Broker Details Group JSON model
  * @author lawrence.daniels@gmail.com
  */
case class BrokerDetailsGroupJs(host: String, details: Seq[BrokerDetailsJs])

/**
  * Broker Details Group JSON Companion Object
  * @author lawrence.daniels@gmail.com
  */
object BrokerDetailsGroupJs {

  implicit val BrokerDetailsGroupReads = Json.reads[BrokerDetailsGroupJs]

  implicit val BrokerDetailsGroupWrites = Json.writes[BrokerDetailsGroupJs]

}
