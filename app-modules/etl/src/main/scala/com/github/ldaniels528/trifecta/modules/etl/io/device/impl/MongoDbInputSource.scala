package com.github.ldaniels528.trifecta.modules.etl.io.device.impl

import java.util.UUID

import com.github.ldaniels528.commons.helpers.OptionHelper._
import com.github.ldaniels528.trifecta.modules.etl.io.Scope
import com.github.ldaniels528.trifecta.modules.etl.io.device.impl.MongoDbSource._
import com.github.ldaniels528.trifecta.modules.etl.io.device.{DataSet, InputSource}
import com.github.ldaniels528.trifecta.modules.etl.io.layout.Layout
import com.github.ldaniels528.trifecta.modules.etl.io.record.Record
import com.mongodb.casbah.Imports._

import scala.collection.JavaConversions._

/**
  * MongoDB Input Source
  * @author lawrence.daniels@gmail.com
  */
case class MongoDbInputSource(id: String, serverList: String, database: String, collection: String, layout: Layout)
  extends InputSource with MongoDbSource {

  private val connUUID = UUID.randomUUID()
  private val collUUID = UUID.randomUUID()
  private val cursorUUID = UUID.randomUUID()

  override def open(implicit scope: Scope) = {
    scope ++= Seq(
      "flow.output.database" -> database,
      "flow.output.collection" -> collection,
      "flow.output.servers" -> serverList,
      "flow.output.count" -> (() => getStatistics.count),
      "flow.output.offset" -> (() => getStatistics.offset)
    )

    val conn = scope.createResource(connUUID, MongoConnection(makeServerList(serverList)))
    val mc = scope.createResource(collUUID, conn(database)(collection))
    scope.createResource(cursorUUID, mc.find(MongoDBObject())) // TODO add a query
    ()
  }

  override def close(implicit scope: Scope) = {
    scope.discardResource[MongoConnection](connUUID).foreach(_.close())
  }

  override def read(record: Record)(implicit scope: Scope) = {
    (scope.getResource[Iterator[DBObject]](cursorUUID) map toDataSet) orDie "No MongoDB connection found"
  }

  private def toDataSet(it: Iterator[DBObject])(implicit scope: Scope): Option[DataSet] = {
    if (!it.hasNext) None
    else {
      val doc = it.next()
      val dataSet = DataSet(doc.keySet().map(key => key -> Option(doc.get(key))).toSeq)
      updateCount(1)
      Some(dataSet)
    }
  }

}

