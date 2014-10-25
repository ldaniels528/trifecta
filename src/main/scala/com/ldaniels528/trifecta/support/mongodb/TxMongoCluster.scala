package com.ldaniels528.trifecta.support.mongodb

import com.mongodb.casbah.Imports._
import com.mongodb.casbah.commons.conversions.scala._
import com.mongodb.{MongoOptions, ServerAddress}

/**
 * Trifecta MongoDB Client
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
case class TxMongoCluster(servers: String) {

  // register the time/date helpers
  RegisterJodaTimeConversionHelpers()

  def close(): Unit = ()

  /**
   * Creates a new database connection
   */
  def connect(databaseName: String): TxMongoDB = {
    // create the options
    val options = new MongoOptions(MongoClientOptions.Defaults)

    // create the connection
    TxMongoDB(databaseName, MongoConnection(makeServerList(servers), options))
  }

  private def makeServerList(hosts: String): List[ServerAddress] = {
    hosts.split("[,]").toList flatMap { pair =>
      pair.split("[:]").toList match {
        case host :: port :: Nil => Option(new ServerAddress(host, port.toInt))
        case host :: Nil => Option(new ServerAddress(host, 27017))
        case _ => None
      }
    }
  }

}