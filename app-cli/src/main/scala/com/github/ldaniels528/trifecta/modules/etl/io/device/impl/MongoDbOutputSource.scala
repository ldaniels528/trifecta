package com.github.ldaniels528.trifecta.modules.etl.io.device.impl

import java.util.UUID

import com.github.ldaniels528.trifecta.modules.etl.io.Scope
import com.github.ldaniels528.trifecta.modules.etl.io.device.{DataSet, OutputSource}
import com.github.ldaniels528.trifecta.modules.etl.io.device.impl.MongoDbOutputSource._
import com.github.ldaniels528.trifecta.modules.etl.io.layout._
import com.github.ldaniels528.trifecta.modules.etl.io.record.Record
import com.mongodb.ServerAddress
import com.mongodb.casbah.Imports._
import com.mongodb.casbah.MongoCollection
import com.mongodb.casbah.commons.conversions.scala.RegisterJodaTimeConversionHelpers
import play.api.libs.json.{JsBoolean, _}

/**
  * MongoDB Output Source
  */
case class MongoDbOutputSource(id: String, serverList: String, database: String, collection: String, writeConcern: WriteConcern, layout: Layout)
  extends OutputSource {

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
    (for {
      mc <- scope.getResource[MongoCollection](collUUID)
    } yield {
      val doc = toDocument(dataSet.convertToJson(record))
      val result = mc.insert(doc, writeConcern)
      updateCount(if (result.wasAcknowledged()) result.getN else 1)
    }) getOrElse 0
  }

  private def toDocument(js: JsObject) = {
    js.fieldSet.foldLeft(DBObject()) { case (dbo, (name, jv)) =>
      dbo.put(name, unwrap(jv))
      dbo
    }
  }

  private def unwrap(jv: JsValue): AnyRef = {
    jv match {
      case ja: JsArray => ja.value.map(unwrap)
      case jb: JsBoolean => jb.value: java.lang.Boolean
      case jn: JsNumber => jn.value.toDouble: java.lang.Double
      case js: JsString => js.value
      case ju =>
        throw new IllegalStateException(s"Unable to unwrap '$ju' (${Option(ju).map(_.getClass.getName).orNull})")
    }
  }

}

/**
  * MongoDB Output Source Companion Object
  */
object MongoDbOutputSource {

  // register the time/date helpers
  RegisterJodaTimeConversionHelpers()

  /**
    * Creates a collection of server address instances from the given hosts string
    * @param hosts given hosts string (e.g. "server1:27017,server2:27017,server3:27018")
    * @return a collection of [[ServerAddress server address]] instances
    */
  def makeServerList(hosts: String): List[ServerAddress] = {
    hosts.split("[,]").toList flatMap { pair =>
      pair.split("[:]").toList match {
        case host :: port :: Nil => Option(new ServerAddress(host, port.toInt))
        case host :: Nil => Option(new ServerAddress(host, 27017))
        case _ => None
      }
    }
  }

}
