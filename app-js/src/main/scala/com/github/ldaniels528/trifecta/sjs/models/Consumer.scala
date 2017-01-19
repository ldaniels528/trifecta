package com.github.ldaniels528.trifecta.sjs.models

import org.scalajs.angularjs.angular
import org.scalajs.dom.browser.console

import scala.concurrent.duration.FiniteDuration
import scala.scalajs.js
import scala.scalajs.js.annotation.ScalaJSDefined

/**
  * Represents a Kafka Consumer Group
  * @author lawrence.daniels@gmail.com
  */
@ScalaJSDefined
class Consumer(var consumerId: js.UndefOr[String] = js.undefined,
               var offsets: js.UndefOr[js.Array[ConsumerOffset]] = js.undefined,
               var owners: js.UndefOr[js.Array[ConsumerOwner]] = js.undefined,
               var threads: js.UndefOr[js.Array[ConsumerThread]] = js.undefined) extends js.Object {

  var expanded: js.UndefOr[Boolean] = js.undefined
  var loading: js.UndefOr[Boolean] = js.undefined

}

/**
  * Consumer Companion
  * @author lawrence.daniels@gmail.com
  */
object Consumer {

  implicit class ConsumerEnrichment(val consumer: Consumer) extends AnyVal {

    def isUpdatedSince(duration: FiniteDuration): Boolean = {
      val cutOffTime = js.Date.now() - duration.toMillis
      (for {
        offsets <- consumer.offsets.toList
        offset <- offsets.toList
        lastModified <- offset.lastModifiedTime.toList
      } yield lastModified >= cutOffTime).contains(true)
    }

  }

  implicit class ConsumerArrayEnrichment(val consumers: js.Array[Consumer]) extends AnyVal {

    def updateDelta(delta: ConsumerDelta) {
      // attempt to find the consumer offset to update
      val result = for {
        consumer <- consumers.find(_.consumerId == delta.consumerId)
        offsets <- consumer.offsets.toOption
        offset <- offsets.find(o => o.topic == delta.topic && o.partition == delta.partition)
      } yield (consumer, offset)

      result match {
        case Some((consumer, offset)) =>
          console.log(s"delta = ${angular.toJson(delta)}")
          offset.deltaC = for (offset1 <- delta.offset.map(_.toDouble); offset0 <- offset.offset) yield Math.abs(offset1 - offset0)
          offset.deltaT = for (offset1 <- delta.topicOffset.map(_.toDouble); offset0 <- offset.topicEndOffset) yield Math.abs(offset1 - offset0)
          offset.offset = delta.offset.map(_.toDouble)
          console.log(s"${consumer.consumerId}: offset = ${offset.offset}, deltaC = ${offset.deltaC}, deltaT = ${offset.deltaT}")
        case None =>
          console.warn(s"${delta.consumerId} could not be updated: ${angular.toJson(delta)}")
      }
    }

  }

}