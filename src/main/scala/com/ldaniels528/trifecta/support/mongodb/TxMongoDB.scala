package com.ldaniels528.trifecta.support.mongodb

import com.mongodb.casbah.Imports._

import scala.util.Try

/**
 * Trifecta MongoDB Database Connection
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
case class TxMongoDB(databaseName: String, conn: MongoConnection) {

  /**
   * Closes the database connection
   */
  def close() {
    Try(conn.close())
    ()
  }

  /**
   * Returns a new reference to the specified collection
   */
  def getCollection(name: String): TxMongoCollection = TxMongoCollection(conn(databaseName)(name))

}
