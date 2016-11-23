package com.github.ldaniels528.trifecta.modules.etl.io.device.impl

import java.util.UUID

import com.github.ldaniels528.commons.helpers.OptionHelper._
import com.github.ldaniels528.trifecta.modules.etl.io.Scope
import com.github.ldaniels528.trifecta.modules.etl.io.device.impl.DocumentDBOutputSource.DocumentDBConnectionInfo
import com.github.ldaniels528.trifecta.modules.etl.io.device.{DataSet, OutputSource}
import com.github.ldaniels528.trifecta.modules.etl.io.layout.Layout
import com.github.ldaniels528.trifecta.modules.etl.io.record.Record
import com.microsoft.azure.documentdb._

import scala.collection.JavaConversions._

/**
  * Azure DocumentDB Output Source
  * @author lawrence.daniels@gmail.com
  */
case class DocumentDBOutputSource(id: String, options: DocumentDBConnectionInfo, layout: Layout) extends OutputSource {

  private val uuid = UUID.randomUUID()
  private var docCollLink: Option[String] = None

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
    val client = scope.createResource(uuid, options.connect)

    // lookup the database and the document collection
    val database = lookupDatabases(client, options.getDatabase).headOption orDie "Database not found"
    val collection = lookupCollections(client, database, options.getCollection).headOption orDie "Document collection not found"
    docCollLink = Option(collection.getSelfLink)

    //client.createDatabase(new Database("""{ "id": "broadway" }"""), new RequestOptions())
    //client.createCollection(database.getSelfLink, new DocumentCollection("""{ "id": "powerball_history" } """), requestOptions).wait()
    ()
  }

  override def writeRecord(record: Record, dataSet: DataSet)(implicit scope: Scope) = {
    scope.getResource[DocumentClient](uuid) map { client =>
      val link = docCollLink orDie "Document collection link not set"
      val doc = new Document(dataSet.convertToJson(record).toString())
      val options = new RequestOptions()
      val disableAutomaticIdGen = false
      val response = client.createDocument(link, doc, options, disableAutomaticIdGen)
      updateCount(1)
    } orDie s"DocumentDB output source '$id' has not been opened"
  }

  private def lookupDatabases(client: DocumentClient, name: String) = {
    val feedResponse = client.queryDatabases(s"SELECT * FROM root r WHERE r.id = '$name'", new FeedOptions())
    feedResponse.getQueryIterator.toSeq
  }

  private def lookupCollections(client: DocumentClient, database: Database, name: String) = {
    val feedResponse = client.queryCollections(database.getSelfLink, s"SELECT * FROM root r WHERE r.id = '$name'", new FeedOptions())
    feedResponse.getQueryIterator.toSeq
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
                                      database: String,
                                      collection: String,
                                      consistencyLevel: ConsistencyLevel) {

    def connect(implicit scope: Scope) = {
      new DocumentClient(
        scope.evaluateAsString(host),
        scope.evaluateAsString(masterKey),
        ConnectionPolicy.GetDefault(),
        ConsistencyLevel.Session)
    }

    def getDatabase(implicit scope: Scope) = scope.evaluateAsString(database)

    def getCollection(implicit scope: Scope) = scope.evaluateAsString(collection)
  }

}