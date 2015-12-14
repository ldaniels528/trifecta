package com.github.ldaniels528.trifecta.models

import play.api.libs.json.Json

/**
  * Replica Host JSON model
  * @author lawrence.daniels@gmail.com
  */
case class ReplicaHostJs(host: String, inSync: Boolean)

/**
  * Replica Host JSON Companion Object
  * @author lawrence.daniels@gmail.com
  */
object ReplicaHostJs {

  implicit val ReplicaHostReads = Json.reads[ReplicaHostJs]

  implicit val ReplicaHostWrites = Json.writes[ReplicaHostJs]
}
