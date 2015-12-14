package com.github.ldaniels528.trifecta.models

import play.api.libs.json.Json

/**
  * Replica JSON model
  * @author lawrence.daniels@gmail.com
  */
case class ReplicaJs(partition: Int, replicas: Seq[ReplicaHostJs])

/**
  * Replica JSON Companion Object
  * @author lawrence.daniels@gmail.com
  */
object ReplicaJs {

  implicit val ReplicaReads = Json.reads[ReplicaJs]

  implicit val ReplicaWrites = Json.writes[ReplicaJs]


}
