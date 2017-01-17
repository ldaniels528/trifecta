package com.github.ldaniels528.trifecta.sjs.models

import org.scalajs.angularjs._

import scala.concurrent.duration._
import scala.scalajs.js
import scala.scalajs.js.annotation.ScalaJSDefined

/**
  * Represents Consumer Details
  * @author lawrence.daniels@gmail.com
  */
@ScalaJSDefined
class Consumer(var deltaC: js.UndefOr[Int] = js.undefined,
               var deltaT: js.UndefOr[Int] = js.undefined) extends ConsumerDelta

/**
  * Consumer Companion Object
  * @author lawrence.daniels@gmail.com
  */
object Consumer {

  def apply(delta: ConsumerDelta): Consumer = {
    new ConsumerDelta(
      consumerId = delta.consumerId,
      topic = delta.topic,
      partition = delta.partition,
      offset = delta.offset,
      lastModified = delta.lastModified,
      lastModifiedISO = delta.lastModifiedISO,
      messagesLeft = delta.messagesLeft,
      topicOffset = delta.topicOffset
    ).asInstanceOf[Consumer]
  }

  /**
    * Consumer Enrichment
    * @param consumer the given [[Consumer consumer]]
    */
  implicit class ConsumerEnrichment(val consumer: Consumer) extends AnyVal {

    def isUpdatedSince(duration: FiniteDuration): Boolean = {
      val cutOffTime = new js.Date().getTime() - duration
      consumer.lastModified.exists(_ >= cutOffTime)
    }

    def update(delta: ConsumerDelta) {
      consumer.deltaC = for (dOffset <- delta.offset; cOffset <- consumer.offset) yield Math.abs(dOffset - cOffset)
      consumer.deltaT = for (dOffset <- delta.topicOffset; cOffset <- consumer.topicOffset) yield Math.abs(dOffset - cOffset)
      consumer.lastModified = delta.lastModified
      consumer.lastModifiedISO = delta.lastModifiedISO
      consumer.messagesLeft = delta.messagesLeft
      consumer.offset = delta.offset
      consumer.topicOffset = delta.topicOffset
    }
  }

}