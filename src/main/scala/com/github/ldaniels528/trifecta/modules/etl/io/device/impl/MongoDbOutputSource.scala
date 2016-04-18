package com.github.ldaniels528.trifecta.modules.etl.io.device.impl

import java.util.UUID

import com.github.ldaniels528.commons.helpers.OptionHelper._
import com.github.ldaniels528.trifecta.modules.etl.io.Scope
import com.github.ldaniels528.trifecta.modules.etl.io.device.impl.MongoDbSource._
import com.github.ldaniels528.trifecta.modules.etl.io.device.{DataSet, OutputSource}
import com.github.ldaniels528.trifecta.modules.etl.io.layout._
import com.github.ldaniels528.trifecta.modules.etl.io.record.Record
import com.mongodb.casbah.Imports._
import com.mongodb.casbah.MongoCollection

/**
  * MongoDB Output Source
  * @author lawrence.daniels@gmail.com
  */
case class MongoDbOutputSource(id: String, serverList: String, database: String, collection: String, writeConcern: WriteConcern, layout: Layout)
  extends OutputSource with MongoDbSource {

  private val connUUID = UUID.randomUUID()
  private val collUUID = UUID.randomUUID()

  override def open(implicit scope: Scope) = {
    scope ++= Seq(
      "flow.output.database" -> database,
      "flow.output.collection" -> collection,
      "flow.output.servers" -> serverList,
      "flow.output.writeConcern" -> writeConcern.toString,
      "flow.output.count" -> (() => getStatistics.count),
      "flow.output.offset" -> (() => getStatistics.offset)
    )

    val mongoConn = scope.createResource(connUUID, MongoConnection(makeServerList(serverList)))
    scope.createResource(collUUID, mongoConn(database)(collection))
    ()
  }

  override def close(implicit scope: Scope) = {
    scope.discardResource[MongoConnection](connUUID).foreach(_.close())
  }

  override def writeRecord(record: Record, dataSet: DataSet)(implicit scope: Scope) = {
    (scope.getResource[MongoCollection](collUUID) map { mc =>
      val doc = toDocument(dataSet.convertToJson(record))
      val result = mc.insert(doc, writeConcern)
      updateCount(if (result.wasAcknowledged()) result.getN else 1)
    }) orDie "No MongoDB connection found"
  }

}
