package com.github.ldaniels528.trifecta.modules.azure

import com.github.ldaniels528.commons.helpers.OptionHelper._
import com.microsoft.azure.documentdb._

import scala.collection.JavaConversions._

/**
  * Represents an Azure DocumentDB Connection
  * @param host             the given host
  * @param masterKey        the given master key
  * @param consistencyLevel the [[ConsistencyLevel consistency level]]
  */
case class TxDocumentDbConnection(host: String,
                                  masterKey: String,
                                  databaseName: String,
                                  collectionName: String,
                                  consistencyLevel: ConsistencyLevel) {

  val client = new DocumentClient(host, masterKey, ConnectionPolicy.GetDefault(), consistencyLevel)
  val database = lookupDatabases(client, databaseName).headOption orDie s"Database '$databaseName' not found"
  val collection = lookupCollections(client, database, collectionName).headOption orDie s"Collection '$collectionName' not found"

  def createDocument(jsonString: String, options: RequestOptions = new RequestOptions(), disableAutomaticIdGen: Boolean = false) = {
    val response = client.createDocument(collection.getSelfLink, new Document(jsonString), options, disableAutomaticIdGen)
    response.getStatusCode
    // TODO handle the response
  }

  def queryDocuments(query: String, options: FeedOptions = new FeedOptions()) = {
    client.queryDocuments(collection.getSelfLink, query, options)
  }

  def close(): Unit = ()

  private def lookupDatabases(client: DocumentClient, name: String) = {
    val feedResponse = client.queryDatabases(s"SELECT * FROM root r WHERE r.id = '$name'", new FeedOptions())
    feedResponse.getQueryIterator.toIterable
  }

  private def lookupCollections(client: DocumentClient, database: Database, name: String) = {
    val feedResponse = client.queryCollections(database.getSelfLink, s"SELECT * FROM root r WHERE r.id = '$name'", new FeedOptions())
    feedResponse.getQueryIterator.toIterable
  }

}