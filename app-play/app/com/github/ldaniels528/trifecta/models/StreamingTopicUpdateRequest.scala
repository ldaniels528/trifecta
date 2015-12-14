package com.github.ldaniels528.trifecta.models

import play.api.libs.json.Json

/**
  * Streaming Topic Update Request
  * @author lawrence.daniels@gmail.com
  */
case class StreamingTopicUpdateRequest(frequency: Int)

/**
  * Streaming Topic Update Request
  * @author lawrence.daniels@gmail.com
  */
object StreamingTopicUpdateRequest {

  implicit val StreamingTopicUpdateRequestReads = Json.reads[StreamingTopicUpdateRequest]

  implicit val StreamingTopicUpdateRequestWrites = Json.writes[StreamingTopicUpdateRequest]

}
