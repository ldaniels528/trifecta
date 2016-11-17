package com.github.ldaniels528.trifecta.sjs.models

import org.scalajs.angularjs._
import org.scalajs.nodejs.util.ScalaJsHelper._

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
    val consumer = New[Consumer]
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
      consumer.deltaC = for(dOffset <- delta.offset; cOffset <- consumer.offset) yield Math.abs(dOffset - cOffset)
      consumer.deltaT = for(dOffset <- delta.topicOffset; cOffset <- consumer.topicOffset) yield Math.abs(dOffset - cOffset)
      consumer.lastModified = delta.lastModified
      consumer.messagesLeft = delta.messagesLeft
      consumer.offset = delta.offset
      consumer.topicOffset = delta.topicOffset
    }
  }

}