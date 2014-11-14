package com.ldaniels528.trifecta.modules.mongodb

import com.ldaniels528.trifecta.command.{Command, UnixLikeArgs, UnixLikeParams}
import com.ldaniels528.trifecta.modules.Module
import com.ldaniels528.trifecta.io.{InputSource, KeyAndMessage}
import com.ldaniels528.trifecta.io.json.TxJsonUtil._
import com.ldaniels528.trifecta.messages.MessageDecoder
import com.ldaniels528.trifecta.io.mongodb.{MongoOutputSource, TxMongoCluster, TxMongoDB}
import com.ldaniels528.trifecta.util.BinaryMessaging
import com.ldaniels528.trifecta.util.ParsingHelper._
import com.ldaniels528.trifecta.util.TxUtils._
import com.ldaniels528.trifecta.vscript.Variable
import com.ldaniels528.trifecta.{TxConfig, TxRuntimeContext}
import com.mongodb.WriteResult
import net.liftweb.json.JsonAST.JValue

import scala.concurrent.ExecutionContext.Implicits.global

/**
 * MongoDB Module
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class MongoModule(config: TxConfig) extends Module with BinaryMessaging {
  private var cluster_? : Option[TxMongoCluster] = None
  private var database_? : Option[TxMongoDB] = None

  /**
   * Returns the commands that are bound to the module
   * @return the commands that are bound to the module
   */
  override def getCommands(implicit rt: TxRuntimeContext): Seq[Command] = Seq(
    Command(this, "mconnect", connect, UnixLikeParams(Seq("host" -> false, "port" -> false)), help = "Establishes a connection to a MongoDB cluster"),
    Command(this, "mfindone", getDocument, UnixLikeParams(Seq("collection" -> true, "query" -> true, "fields" -> false), Seq("-o" -> "outputSource")), help = "Retrieves a document from MongoDB"),
    Command(this, "mget", getDocument, UnixLikeParams(Seq("collection" -> true, "query" -> true, "fields" -> false), Seq("-o" -> "outputSource")), help = "Retrieves a document from MongoDB"),
    Command(this, "mput", insertDocument, UnixLikeParams(Seq("collection" -> true, "json" -> true)), help = "Inserts a document into MongoDB"),
    Command(this, "use", selectDatabase, UnixLikeParams(Seq("database" -> true)), help = "Sets the current MongoDB database")
  )

  /**
   * Returns the variables that are bound to the module
   * @return the variables that are bound to the module
   */
  override def getVariables: Seq[Variable] = Nil

  /**
   * Attempts to retrieve an input source for the given URL
   * @param url the given input URL
   * @return the option of an input source
   */
  override def getInputSource(url: String): Option[InputSource] = None

  /**
   * Attempts to retrieve an output source for the given URL
   * @param url the given output URL
   * @return the option of an output source
   */
  override def getOutputSource(url: String): Option[MongoOutputSource] = {
    for {
      databaseName <- url.extractProperty("mongo:")
      db <- database_?
      mc = db.getCollection(databaseName)
    } yield new MongoOutputSource(mc)
  }

  /**
   * Returns the name of the prefix (e.g. Seq("file"))
   * @return the name of the prefix
   */
  override def supportedPrefixes: Seq[String] = Seq("mongo")

  /**
   * Returns the label of the module (e.g. "kafka")
   * @return the label of the module
   */
  override def moduleLabel: String = "mongo"

  /**
   * Returns the name of the module (e.g. "kafka")
   * @return the name of the module
   */
  override def moduleName: String = "mongodb"

  /**
   * Called when the application is shutting down
   */
  override def shutdown(): Unit = ()

  /**
   * Establishes a connection to a remote MongoDB cluster
   * @example mconnect dev528
   */
  def connect(params: UnixLikeArgs): Unit = {
    // determine the requested end-point
    val hosts = params.args match {
      case Nil => config.configProps.getProperty("trifecta.mongodb.hosts", "localhost:27017")
      case path :: Nil => s"$path:27017"
      case path :: port :: Nil => s"$path:${parsePort(port)}"
      case _ => dieSyntax(params)
    }

    // connect to the remote peer
    cluster_?.foreach(_.close())
    cluster_? = Option(TxMongoCluster(hosts))
  }

  /**
   * Retrieves a MongoDB document
   * @example mget Stocks { "symbol" : "AAPL" }
   * @example mget Stocks { "symbol" : "AAPL" } { "symbol":true, "exchange":true, "lastTrade":true }
   * @example mget Stocks { "symbol" : "AAPL" } -o es:/quotes/quote/AAPL
   */
  def getDocument(params: UnixLikeArgs)(implicit rt: TxRuntimeContext): Option[JValue] = {
    // retrieve the document from the collection
    val doc = params.args match {
      case List(tableName, query) => database.getCollection(tableName).findOne(toJson(query))
      case List(tableName, query, fields) => database.getCollection(tableName).findOne(toJson(query), toJson(fields))
      case _ => dieSyntax(params)
    }

    // determine which decoder to use; either the user specified decoder, cursor's decoder or none
    val decoder: Option[MessageDecoder[_]] = params("-a") map (lookupAvroDecoder(_)(config))

    // write the document to an output source?
    for {
      mydoc <- doc
      outputSource <- getOutputSource(params)
    } {
      val key = hexToBytes((mydoc \ "_id" \ "$oid").values.toString)
      val value = mydoc.toString.getBytes(config.encoding)
      outputSource.write(KeyAndMessage(key, value), decoder)
    }

    doc
  }

  /**
   * Inserts a document into the database
   * @example mput quotes { "symbol" : "AAPL", "lastTrade":99.56, "exchange":"NYSE" }
   */
  def insertDocument(params: UnixLikeArgs): WriteResult = {
    val (tableName, json) = params.args match {
      case List(collectionName, jsonString) => (collectionName, toJson(jsonString))
      case _ => dieSyntax(params)
    }

    // insert the document into the collection
    database.getCollection(tableName).insert(json)
  }

  /**
   * Selects the current database
   * @example use shocktrade
   * @example mgo:use shocktrade
   */
  def selectDatabase(params: UnixLikeArgs): Unit = {
    val databaseName = params.args match {
      case List(aDatabaseName) => aDatabaseName
      case _ => dieSyntax(params)
    }

    // select the database
    database_?.foreach(_.close())
    database_? = Option(cluster.connect(databaseName))
  }

  private def cluster: TxMongoCluster = {
    val myCluster_? : Option[TxMongoCluster] =
      if (cluster_?.isDefined) cluster_?
      else {
        val hosts = config.configProps.getProperty("trifecta.mongodb.hosts", "localhost")
        Option(TxMongoCluster(hosts))
      }

    myCluster_? getOrElse die("No MongoDB cluster defined; use 'mconnect <hosts>'")
  }

  private def database: TxMongoDB = database_? getOrElse die("No MongoDB database defined; use 'use <databseName>'")

}
