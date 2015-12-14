package com.github.ldaniels528.trifecta.models

import play.api.libs.json.Json

/**
  * Leader And Replicas JSON model
  * @author lawrence.daniels@gmail.com
  */
case class LeaderAndReplicasJs(partition: Int, leader: BrokerJs, replica: BrokerJs)

/**
  * Leader And Replicas JSON Companion Object
  * @author lawrence.daniels@gmail.com
  */
object LeaderAndReplicasJs {

  implicit val LeaderAndReplicasReads = Json.reads[LeaderAndReplicasJs]

  implicit val LeaderAndReplicasWrites = Json.writes[LeaderAndReplicasJs]

}
