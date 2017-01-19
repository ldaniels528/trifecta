package com.github.ldaniels528.trifecta.sjs.models

import com.github.ldaniels528.trifecta.sjs.models.Query.SavedResult
import org.scalajs.dom.browser.console
import org.scalajs.sjs.JsUnderOrHelper._

import scala.scalajs.js
import scala.scalajs.js.JSConverters._

/**
  * Represents Topic Details
  * @author lawrence.daniels@gmail.com
  */
@js.native
trait TopicDetails extends js.Object {
  var topic: String = js.native
  var partitions: js.Array[PartitionDetails] = js.native
  var leader: js.Array[Broker] = js.native
  var replicas: js.Array[ReplicaGroup] = js.native
  var totalMessages: Int = js.native

  // ui-specific properties
  var expanded: js.UndefOr[Boolean] = js.native
  var loading: js.UndefOr[Boolean] = js.native
  var loadingConsumers: js.UndefOr[Boolean] = js.native
  var totalMessagesDelta: js.UndefOr[Int] = js.native
  var queriesExpanded: js.UndefOr[Boolean] = js.native
  var query: js.UndefOr[Query] = js.native
  var queryResults: js.UndefOr[js.Array[SavedResult]] = js.native
  var replicaExpanded: js.UndefOr[Boolean] = js.native
  var updatingTopics: js.UndefOr[Int] = js.native

}

/**
  * Topic Details Companion Object
  * @author lawrence.daniels@gmail.com
  */
object TopicDetails {

  /**
    * Topic Details Enrichment
    * @author lawrence.daniels@gmail.com
    */
  final implicit class TopicDetailsEnrichment(val details: TopicDetails) extends AnyVal {

    @inline
    def apply(partitionId: Int): Option[PartitionDetails] = {
      details.partitions.find(_.partition == partitionId)
    }

    def replace(delta: PartitionDelta) {
      for {
        partition <- delta.partition
        myDelta <- details(partition).orUndefined
      } {
        // set the total messages
        details.totalMessagesDelta = delta.totalMessages.map(_ - details.totalMessages)
        delta.totalMessages.foreach(details.totalMessages = _)

        // update the my partition's detail
        myDelta.delta = for (offset1 <- delta.endOffset; offset0 <- myDelta.endOffset) yield offset1 - offset0
        myDelta.startOffset = delta.startOffset
        myDelta.endOffset = delta.endOffset
        myDelta.messages = delta.messages
        myDelta.totalMessages = delta.totalMessages
      }
    }

    def replace(message: Message) {
      for {
        partitionId <- message.partition
      } {
        details.partitions.indexWhere(_.partition.contains(partitionId)) match {
          case -1 =>
            console.warn(s"Partition $partitionId does not exist for topic ${details.topic}")
          case index =>
            details.partitions(index).offset = message.offset
        }
      }
    }

  }

}