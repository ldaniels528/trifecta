package com.github.ldaniels528.trifecta.modules.mongodb

import com.github.ldaniels528.trifecta.io.json.JsonHelper
import com.mongodb.casbah.Imports.{DBObject, _}
import com.mongodb.casbah.MongoCollection
import net.liftweb.json.JsonAST.JValue

/**
 * Trifecta MongoDB Collection
 * @author lawrence.daniels@gmail.com
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

  /**
    * Converts the given JSON value into a MongoDB document
    * @param js the given [[JValue]]
    * @return the resultant MongoDB document
    */
  def toDocument(js: JValue): DBObject = {
    js.values match {
      case m: Map[_, _] =>
        val mapping = m.map { case (k, v) => (String.valueOf(k), v) }
        convertToMDB(mapping).asInstanceOf[DBObject]
      case x => throw new IllegalArgumentException(s"$x (${Option(x).map(_.getClass.getName)})")
    }
  }

  /**
    * Converts the given MongoDB document into a JSON value
    * @param result the given  MongoDB document
    * @return the resultant [[JValue]]
    */
  def toJson(result: DBObject): JValue = JsonHelper.toJson(result.toString)

  private def convertToMDB[T](input: T): Any = {
    input match {
      case m: Map[_, _] =>
        val mapping = m.map { case (k, v) => (String.valueOf(k), v) }
        mapping.foldLeft(DBObject()) { case (result, (key, value)) =>
          result ++ DBObject(key -> convertToMDB(value))
        }
      case x => x
    }
  }

}
