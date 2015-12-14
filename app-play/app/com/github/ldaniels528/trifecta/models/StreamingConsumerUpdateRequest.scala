package com.github.ldaniels528.trifecta.models

import play.api.libs.json.Json

/**
  * Represents a Streaming Consumer Update Request
  * @author lawrence.daniels@gmail.com
  */
case class StreamingConsumerUpdateRequest(frequency: Int)

/**
  * Streaming Consumer Update Request
  * @author lawrence.daniels@gmail.com
  */
object StreamingConsumerUpdateRequest {

  implicit val StreamingConsumerUpdateRequestReads = Json.reads[StreamingConsumerUpdateRequest]

  implicit val StreamingConsumerUpdateRequestWrites = Json.writes[StreamingConsumerUpdateRequest]

}
