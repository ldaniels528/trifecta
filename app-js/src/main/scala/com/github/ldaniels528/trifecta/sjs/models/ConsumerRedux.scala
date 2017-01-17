package com.github.ldaniels528.trifecta.sjs.models

import com.github.ldaniels528.trifecta.sjs.models.ConsumerRedux._
import org.scalajs.angularjs.angular
import org.scalajs.dom.browser.console

import scala.scalajs.js
import scala.scalajs.js.annotation.ScalaJSDefined

/**
  * Consumer Redux
  * @author lawrence.daniels@gmail.com
  */
@ScalaJSDefined
class ConsumerRedux(var consumerId: js.UndefOr[String] = js.undefined,
                    var offsets: js.UndefOr[js.Array[ConsumerOffsetRedux]] = js.undefined,
                    var owners: js.UndefOr[js.Array[ConsumerOwnerRedux]] = js.undefined,
                    var threads: js.UndefOr[js.Array[ConsumerThreadRedux]] = js.undefined) extends js.Object {

  var expanded: js.UndefOr[Boolean] = js.undefined
  var loading: js.UndefOr[Boolean] = js.undefined

}

/**
  * Consumer Redux
  * @author lawrence.daniels@gmail.com
  */
object ConsumerRedux {

  implicit class ConsumerArrayReduxEnrichment(val consumers: js.Array[ConsumerRedux]) extends AnyVal {

    def updateDelta(delta: ConsumerDelta) {
      // attempt to find the consumer offset to update
      val result = for {
        consumer <- consumers.find(_.consumerId == delta.consumerId)
        offsets <- consumer.offsets.toOption
        offset <- offsets.find(o => o.topic == delta.topic && o.partition == delta.partition)
      } yield offset

      result match {
        case Some(offset) =>
          offset.offset = delta.offset.map(_.toDouble)
          offset.deltaC = for (dOffset <- delta.offset.map(_.toDouble); cOffset <- offset.offset) yield Math.abs(dOffset - cOffset)
          offset.deltaT = for (dOffset <- delta.topicOffset.map(_.toDouble); cOffset <- offset.topicEndOffset) yield Math.abs(dOffset - cOffset)
        case None =>
          console.warn(s"${delta.consumerId} could not be updated: ${angular.toJson(delta)}")
      }
    }

  }

  @ScalaJSDefined
  class ConsumerOffsetRedux(var topic: js.UndefOr[String] = js.undefined,
                            var partition: js.UndefOr[Int] = js.undefined,
                            var offset: js.UndefOr[Double] = js.undefined,
                            var topicStartOffset: js.UndefOr[Double] = js.undefined,
                            var topicEndOffset: js.UndefOr[Double] = js.undefined,
                            var messages: js.UndefOr[Double] = js.undefined,
                            var lastModifiedTime: js.UndefOr[Double] = js.undefined) extends js.Object {

    var expanded: js.UndefOr[Boolean] = js.undefined
    var deltaC: js.UndefOr[Double] = js.undefined
    var deltaT: js.UndefOr[Double] = js.undefined
  }

  @ScalaJSDefined
  class ConsumerOwnerRedux(var groupId: js.UndefOr[String] = js.undefined,
                           var topic: js.UndefOr[String] = js.undefined,
                           var partition: js.UndefOr[Int] = js.undefined,
                           var threadId: js.UndefOr[String] = js.undefined) extends js.Object

  @ScalaJSDefined
  class ConsumerThreadRedux(var groupId: js.UndefOr[String] = js.undefined,
                            var threadId: js.UndefOr[String] = js.undefined,
                            var version: js.UndefOr[Int] = js.undefined,
                            var topic: js.UndefOr[String] = js.undefined,
                            var timestamp: js.UndefOr[String] = js.undefined,
                            var timestampISO: js.UndefOr[String] = js.undefined) extends js.Object

}