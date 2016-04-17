package com.github.ldaniels528.trifecta.io.mongodb

import com.mongodb.ServerAddress
import com.mongodb.casbah.Imports._
import com.mongodb.casbah.commons.conversions.scala._

/**
  * Trifecta MongoDB Client
  * @author lawrence.daniels@gmail.com
  */
case class TxMongoCluster(servers: String) {

  // register the time/date helpers
  RegisterJodaTimeConversionHelpers()

  /**
    * Closes the Mongo cluster instance
    */
  def close(): Unit = ()

  /**
    * Creates a new database connection
    * @param databaseName the given database name
    * @return a new [[TxMongoDB MongoDB connection]]
    */
  def connect(databaseName: String): TxMongoDB = {
    TxMongoDB(databaseName, MongoConnection(makeServerList(servers)))
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