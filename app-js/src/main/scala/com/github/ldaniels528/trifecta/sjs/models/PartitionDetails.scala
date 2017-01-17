package com.github.ldaniels528.trifecta.sjs.models

import org.scalajs.nodejs.util.ScalaJsHelper._
import org.scalajs.sjs.JsUnderOrHelper._

import scala.scalajs.js
import scala.scalajs.js.annotation.ScalaJSDefined

/**
  * Partition Details
  * @author lawrence.daniels@gmail.com
  */
@ScalaJSDefined
class PartitionDetails(var partition: js.UndefOr[Int] = js.undefined,
                       var startOffset: js.UndefOr[Int] = js.undefined,
                       var endOffset: js.UndefOr[Int] = js.undefined,
                       var offset: js.UndefOr[Int] = js.undefined,
                       var delta: js.UndefOr[Int] = js.undefined,
                       var messages: js.UndefOr[Int] = js.undefined,
                       var totalMessages: js.UndefOr[Int] = js.undefined) extends js.Object

/**
  * Partition Details Companion Object
  * @author lawrence.daniels@gmail.com
  */
object PartitionDetails {

  def apply(delta: PartitionDelta) = new PartitionDetails(
    partition = delta.partition,
    offset = delta.startOffset,
    startOffset = delta.startOffset,
    endOffset = delta.endOffset,
    messages = delta.messages,
    totalMessages = delta.totalMessages
  )

  /**
    * Partition Details Enrichment
    * @author lawrence.daniels@gmail.com
    */
  implicit class PartitionDetailsEnrichment(val aPartition: PartitionDetails) extends AnyVal {

    @inline
    def copy(topic: js.UndefOr[String] = js.undefined,
             partition: js.UndefOr[Int] = js.undefined,
             startOffset: js.UndefOr[Int] = js.undefined,
             endOffset: js.UndefOr[Int] = js.undefined,
             messages: js.UndefOr[Int] = js.undefined,
             totalMessages: js.UndefOr[Int] = js.undefined,
             offset: js.UndefOr[Int] = js.undefined,
             delta: js.UndefOr[Int] = js.undefined): PartitionDetails = {
      val newPartition = New[PartitionDetails]
      newPartition.partition = partition ?? aPartition.partition
      newPartition.offset = offset ?? aPartition.offset
      newPartition.startOffset = startOffset ?? aPartition.startOffset
      newPartition.endOffset = endOffset ?? aPartition.endOffset
      newPartition.messages = messages ?? aPartition.messages
      newPartition.totalMessages = totalMessages ?? aPartition.totalMessages
      newPartition
    }
  }

}