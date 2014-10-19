package com.ldaniels528.trifecta.support.mongodb

import com.ldaniels528.trifecta.util.EndPoint
import com.mongodb.casbah.Imports.{DBObject => Q, _}
import com.mongodb.casbah.commons.conversions.scala._

/**
 * Trifecta MongoDB Client
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class TxMongoCluster(servers: Seq[EndPoint]) {

  // register the time/date helpers
  RegisterJodaTimeConversionHelpers()

  /**
   * Creates a new database connection
   */
  def connect(databaseName: String): TxMongoDB = {
    // create the options
    val options = new MongoOptions()
    options.connectionsPerHost = 100
    options.maxWaitTime = 2000
    options.socketKeepAlive = false
    options.threadsAllowedToBlockForConnectionMultiplier = 50

    // create the connection
    TxMongoDB(databaseName, MongoConnection(servers.map(s => new ServerAddress(s.host, s.port)).toList, options))
  }

}