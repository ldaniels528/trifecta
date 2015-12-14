package com.github.ldaniels528.trifecta.sjs.models

import com.github.ldaniels528.scalascript.util.ScalaJsHelper._

import scala.scalajs.js

/**
  * Partition Details
  * @author lawrence.daniels@gmail.com
  */
@js.native
trait PartitionDetails extends js.Object {
  var partition: js.UndefOr[Int] = js.native
  var startOffset: js.UndefOr[Int] = js.native
  var endOffset: js.UndefOr[Int] = js.native
  var messages: js.UndefOr[Int] = js.native
  var totalMessages: js.UndefOr[Int] = js.native

  // ui properties
  var offset: js.UndefOr[Int] = js.native
  var delta: js.UndefOr[Int] = js.native
}

/**
  * Partition Details Companion Object
  * @author lawrence.daniels@gmail.com
  */
object PartitionDetails {

  def apply(delta: PartitionDelta) = {
    val partition = makeNew[PartitionDetails]
    partition.partition = delta.partition
    partition.offset = delta.endOffset
    partition.startOffset = delta.startOffset
    partition.endOffset = delta.endOffset
    partition.messages = delta.messages
    partition.totalMessages = delta.totalMessages
    partition
  }

  /**
    * Partition Details Enrichment
    * @author lawrence.daniels@gmail.com
    */
  implicit class PartitionDetailsEnrichment(val aPartition: PartitionDetails) extends AnyVal {

    def copy(topic: js.UndefOr[String] = js.undefined,
             partition: js.UndefOr[Int] = js.undefined,
             startOffset: js.UndefOr[Int] = js.undefined,
             endOffset: js.UndefOr[Int] = js.undefined,
             messages: js.UndefOr[Int] = js.undefined,
             totalMessages: js.UndefOr[Int] = js.undefined,
             offset: js.UndefOr[Int] = js.undefined,
             delta: js.UndefOr[Int] = js.undefined) = {
      val newPartition = makeNew[PartitionDetails]
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