package com.github.ldaniels528.trifecta.io.mongodb

import com.mongodb.casbah.Imports._

import scala.collection.concurrent.TrieMap
import scala.util.Try

/**
  * Trifecta MongoDB Database Connection
  * @author lawrence.daniels@gmail.com
  */
case class TxMongoDB(databaseName: String, conn: MongoConnection) {
  private val collections = TrieMap[String, TxMongoCollection]()

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
    collections.getOrElseUpdate(name, TxMongoCollection(conn(databaseName)(name)))
  }

}
