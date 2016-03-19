package com.github.ldaniels528.trifecta.modules.etl.io.device.impl

import java.util.UUID

import com.github.ldaniels528.commons.helpers.OptionHelper._
import com.github.ldaniels528.trifecta.modules.documentdb.TxDocumentDbConnection
import com.github.ldaniels528.trifecta.modules.etl.io.Scope
import com.github.ldaniels528.trifecta.modules.etl.io.device.impl.DocumentDBOutputSource.DocumentDBConnectionInfo
import com.github.ldaniels528.trifecta.modules.etl.io.device.{DataSet, OutputSource}
import com.github.ldaniels528.trifecta.modules.etl.io.layout.Layout
import com.github.ldaniels528.trifecta.modules.etl.io.record.Record
import com.microsoft.azure.documentdb._

/**
  * Azure DocumentDB Output Source
  * @author lawrence.daniels@gmail.com
  */
case class DocumentDBOutputSource(id: String, options: DocumentDBConnectionInfo, layout: Layout) extends OutputSource {
  private val uuid = UUID.randomUUID()

  override def close(implicit scope: Scope) = {
    scope.discardResource[DocumentClient](uuid)
    ()
  }

  override def open(implicit scope: Scope) = {
    scope ++= Seq(
      "flow.output.id" -> id,
      "flow.output.count" -> (() => getStatistics.count),
      "flow.output.offset" -> (() => getStatistics.offset)
    )

    // create the client connection
    scope.createResource(uuid, options.connect)
    ()
  }

  override def writeRecord(record: Record, dataSet: DataSet)(implicit scope: Scope) = {
    scope.getResource[TxDocumentDbConnection](uuid) map { conn =>
      val response = conn.createDocument(dataSet.convertToJson(record).toString())
      // TODO handle the response
      updateCount(1)
    } orDie s"DocumentDB output source '$id' has not been opened"
  }

}

/**
  * DocumentDB Output Source Companion Object
  * @author lawrence.daniels@gmail.com
  */
object DocumentDBOutputSource {

  /**
    * DocumentDB Connection Information
    * @param host             the given host
    * @param masterKey        the given master key
    * @param consistencyLevel the [[ConsistencyLevel consistency level]]
    */
  case class DocumentDBConnectionInfo(host: String,
                                      masterKey: String,
                                      databaseName: String,
                                      collectionName: String,
                                      consistencyLevel: ConsistencyLevel) {

    def connect(implicit scope: Scope) = TxDocumentDbConnection(
      host = scope.evaluateAsString(host),
      masterKey = scope.evaluateAsString(masterKey),
      databaseName = scope.evaluateAsString(databaseName),
      collectionName = scope.evaluateAsString(collectionName),
      consistencyLevel = consistencyLevel
    )

  }

}