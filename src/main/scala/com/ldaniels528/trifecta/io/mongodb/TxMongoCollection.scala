package com.ldaniels528.trifecta.io.mongodb

import com.ldaniels528.trifecta.io.json.JsonHelper._
import com.mongodb.casbah.Imports._
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
    * @return the resultant documents
    */
  def find(): Iterator[JValue] = mc.find().map(toJson)

  /**
   * Retrieves documents matching the given criteria
   * @param criteria the given criteria
   * @return the resultant documents
   */
  def find(criteria: JValue): Iterator[JValue] = {
    mc.find(toDocument(criteria)).map(toJson)
  }

  /**
    * Retrieves documents matching the given criteria
    * @param criteria the given criteria
    * @return the resultant documents
    */
  def find(criteria: JValue, fields: JValue): Iterator[JValue] = {
    mc.find(ref = toDocument(criteria), keys = toDocument(fields)).map(toJson)
  }

  /**
    * Retrieves a document matching the given criteria
    * @return the resultant document
    */
  def findOne(): Option[JValue] = mc.findOne() map toJson

  /**
   * Retrieves a document matching the given criteria
   * @param criteria the given criteria
   * @return the resultant document
   */
  def findOne(criteria: JValue): Option[JValue] = {
    mc.findOne(o = toDocument(criteria)) map toJson
  }

  /**
    * Retrieves a document matching the given criteria
    * @param criteria the given criteria
    * @return the resultant document
    */
  def findOne(criteria: JValue, fields: JValue): Option[JValue] = {
    mc.findOne(o = toDocument(criteria), fields = toDocument(fields)) map toJson
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
