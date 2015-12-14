package com.github.ldaniels528.trifecta.models

import play.api.libs.json.Json

/**
  * Represents a Sampling Start Request
  * @author lawrence.daniels@gmail.com
  */
case class MessageSamplingStartRequest(topic: String, partitionOffsets: List[Long])

/**
  * Sampling Start Request Companion Object
  * @author lawrence.daniels@gmail.com
  */
object MessageSamplingStartRequest {

  implicit val SamplingRequestReads = Json.reads[MessageSamplingStartRequest]

  implicit val SamplingRequestWrites = Json.writes[MessageSamplingStartRequest]
  
}
