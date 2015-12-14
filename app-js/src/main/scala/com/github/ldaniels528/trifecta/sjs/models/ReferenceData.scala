package com.github.ldaniels528.trifecta.sjs.models

import com.github.ldaniels528.scalascript.util.ScalaJsHelper._

import scala.scalajs.js

/**
  * Reference Data
  * @author lawrence.daniels@gmail.com
  */
@js.native
trait ReferenceData extends js.Object {
  var brokers: js.Array[BrokerGroup] = js.native
  var consumers: js.Array[Consumer] = js.native
  var topic: js.UndefOr[TopicDetails] = js.native
  var topics: js.Array[TopicDetails] = js.native

}

/**
  * Reference Data Companion Object
  * @author lawrence.daniels@gmail.com
  */
object ReferenceData {

  def apply(brokers: js.Array[BrokerGroup],
            consumers: js.Array[Consumer],
            topics: js.Array[TopicDetails],
            topic: js.UndefOr[TopicDetails]) = {
    val data = makeNew[ReferenceData]
    data.brokers = brokers
    data.consumers = consumers
    data.topics = topics
    data.topic = topic
    data
  }

}