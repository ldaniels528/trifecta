package com.ldaniels528.trifecta.support.mongodb

import com.mongodb.casbah.Imports._
import com.mongodb.casbah.MongoCollection

/**
 * Trifecta MongoDB Collection
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
case class TxMongoCollection(mc: MongoCollection) {
  var concern = WriteConcern.Safe // Safe | JournalSafe

}
