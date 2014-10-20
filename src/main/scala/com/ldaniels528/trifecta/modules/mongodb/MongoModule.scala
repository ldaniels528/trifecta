package com.ldaniels528.trifecta.modules.mongodb

import com.ldaniels528.trifecta.command.{Command, UnixLikeArgs, UnixLikeParams}
import com.ldaniels528.trifecta.modules.Module
import com.ldaniels528.trifecta.support.io.InputSource
import com.ldaniels528.trifecta.support.json.TxJsonUtil
import com.ldaniels528.trifecta.support.mongodb.{TxMongoCluster, TxMongoDB}
import TxJsonUtil._
import com.ldaniels528.trifecta.util.EndPoint
import com.ldaniels528.trifecta.util.TxUtils._
import com.ldaniels528.trifecta.vscript.Variable
import com.ldaniels528.trifecta.{TxConfig, TxRuntimeContext}

/**
 * MongoDB Module
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class MongoModule(config: TxConfig) extends Module {
  private var cluster_? : Option[TxMongoCluster] = None
  private var database_? : Option[TxMongoDB] = None

  /**
   * Returns the commands that are bound to the module
   * @return the commands that are bound to the module
   */
  override def getCommands(implicit rt: TxRuntimeContext): Seq[Command] = Seq(
    Command(this, "mconnect", connect, UnixLikeParams(Seq("host" -> false, "port" -> false)), help = "Establishes a connection to a MongoDB cluster"),
    Command(this, "mput", insertDocument, UnixLikeParams(Seq("json" -> true)), help = "Inserts a record into MongoDB"),
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
    val endPoint = params.args match {
      case Nil => EndPoint(config.zooKeeperConnect)
      case path :: Nil => EndPoint(path)
      case path :: port :: Nil => EndPoint(path, parseInt("port", port))
      case _ => dieSyntax(params)
    }

    // connect to the remote peer
    //cluster_?.foreach(_.close())
    cluster_? = Option(TxMongoCluster(Seq(endPoint)))
  }

  /**
   * Inserts a document into the database
   * @example mput quotes { "symbol" : "AAPL", "lastTrade":99.56, "exchange":"NYSE" }
   */
  def insertDocument(params: UnixLikeArgs): Unit = {
    val (tableName, json) = params.args match {
      case List(collectionName, jsonString) => (collectionName, toJson(jsonString))
      case _ => dieSyntax(params)
    }

    // insert the record into the collection
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

  private def cluster: TxMongoCluster = cluster_? getOrElse die("No MongoDB cluster defined; use 'mconnect <hosts>'")

  private def database: TxMongoDB = database_? getOrElse die("No MongoDB database defined; use 'use <databseName>'")

}
