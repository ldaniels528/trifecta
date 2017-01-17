package com.github.ldaniels528.trifecta.ui.models

import play.api.libs.json.{Json, Reads, Writes}

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

  implicit val StreamingConsumerUpdateRequestReads: Reads[StreamingConsumerUpdateRequest] = Json.reads[StreamingConsumerUpdateRequest]

  implicit val StreamingConsumerUpdateRequestWrites: Writes[StreamingConsumerUpdateRequest] = Json.writes[StreamingConsumerUpdateRequest]

}
