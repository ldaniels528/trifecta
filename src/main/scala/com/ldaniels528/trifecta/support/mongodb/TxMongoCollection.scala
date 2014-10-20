package com.ldaniels528.trifecta.support.mongodb

import com.ldaniels528.trifecta.support.json.TxJsonUtil._
import com.mongodb.casbah.Imports.{DBObject => Q, _}
import com.mongodb.casbah.MongoCollection
import net.liftweb.json.JsonAST.JValue

/**
 * Trifecta MongoDB Collection
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
case class TxMongoCollection(mc: MongoCollection) {
  var writeConcern: WriteConcern = WriteConcern.Safe // Safe | JournalSafe

  /**
   * Retrieves documents matching the given criteria
   * @param criteria the given criteria
   * @tparam T the template type of the values
   * @return the resultant documents
   */
  def find[T](criteria: JValue) = {
    mc.find(toDocument(criteria))
  }

  /**
   * Retrieves a document matching the given criteria
   * @param criteria the given criteria
   * @tparam T the template type of the values
   * @return the resultant document
   */
  def findOne[T](criteria: JValue) = {
    mc.findOne(toDocument(criteria))
  }

  /**
   * Inserts the given values
   * @param values the given values
   * @tparam T the template type of the values
   * @return the result of the operation
   */
  def insert[T](values: JValue): WriteResult = {
    mc.insert(toDocument(values), writeConcern)
  }

}
