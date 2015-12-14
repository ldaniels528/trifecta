package com.github.ldaniels528.trifecta.models

import play.api.libs.json.Json

/**
  * Zookeeper Item JSON model
  * @author lawrence.daniels@gmail.com
  */
case class ZkItemJs(name: String, path: String)

/**
  * Zookeeper Item JSON Companion Object
  * @author lawrence.daniels@gmail.com
  */
object ZkItemJs {

  implicit val ZkItemReads = Json.reads[ZkItemJs]

  implicit val ZkItemWrites = Json.writes[ZkItemJs]

}
