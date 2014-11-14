package com.ldaniels528.trifecta.io.mongodb

import com.mongodb.casbah.Imports._

import scala.collection.mutable
import scala.util.Try

/**
 * Trifecta MongoDB Database Connection
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
case class TxMongoDB(databaseName: String, conn: MongoConnection) {
  private val collections = mutable.Map[String, TxMongoCollection]()

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
  def getCollection(name: String): TxMongoCollection = {
    collections.getOrElse(name, {
      val collection = TxMongoCollection(conn(databaseName)(name))
      collections += (name -> collection)
      collection
    })
  }

}
