package com.github.ldaniels528.trifecta.models

import play.api.libs.json.Json

/**
  * ZkItemInfo JSON model
  * @author lawrence.daniels@gmail.com
  */
case class ZkItemInfoJs(path: String, creationTime: Option[Long], lastModified: Option[Long], size: Option[Int], data: Option[FormattedDataJs])

/**
  * ZkItemInfo JSON Companion Object
  * @author lawrence.daniels@gmail.com
  */
object ZkItemInfoJs {

  implicit val ZkItemInfoReads = Json.reads[ZkItemInfoJs]

  implicit val ZkItemInfoWrites = Json.writes[ZkItemInfoJs]

}
