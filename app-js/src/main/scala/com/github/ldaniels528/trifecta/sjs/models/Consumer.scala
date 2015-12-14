package com.github.ldaniels528.trifecta.sjs.models

import com.github.ldaniels528.scalascript.core.TimerConversions._
import com.github.ldaniels528.scalascript.util.ScalaJsHelper._

import scala.concurrent.duration._
import scala.scalajs.js

/**
  * Represents Consumer Details
  * @author lawrence.daniels@gmail.com
  */
@js.native
trait Consumer extends ConsumerDelta {
  // ui properties
  var deltaC: js.UndefOr[Int] = js.native
  var deltaT: js.UndefOr[Int] = js.native
}

/**
  * Consumer Companion Object
  * @author lawrence.daniels@gmail.com
  */
object Consumer {

  def apply(delta: ConsumerDelta) = {
    val consumer = makeNew[Consumer]
    consumer.consumerId = delta.consumerId
    consumer.topic = delta.topic
    consumer.partition = delta.partition
    consumer.offset = delta.offset
    consumer.lastModified = delta.lastModified
    consumer.messagesLeft = delta.messagesLeft
    consumer.topicOffset = delta.topicOffset
    consumer
  }

  /**
    * Consumer Enrichment
    * @param consumer the given [[Consumer consumer]]
    */
  implicit class ConsumerEnrichment(val consumer: Consumer) extends AnyVal {

    def isUpdatedSince(duration: FiniteDuration): Boolean = {
      val cutOffTime = new js.Date().getTime() - duration
      consumer.lastModified.map(_.asInstanceOf[ Double]).exists(_ >= cutOffTime)
    }

    def update(delta: ConsumerDelta) {
      consumer.deltaC = Math.abs(delta.offset - consumer.offset)
      consumer.deltaT = Math.abs(delta.topicOffset - consumer.topicOffset)
      consumer.lastModified = delta.lastModified
      consumer.messagesLeft = delta.messagesLeft
      consumer.offset = delta.offset
      consumer.topicOffset = delta.topicOffset
    }
  }

}