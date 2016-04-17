package com.github.ldaniels528.trifecta.modules

import com.github.ldaniels528.trifecta.command.{Command, UnixLikeArgs, UnixLikeParams}
import com.github.ldaniels528.trifecta.io.avro.AvroCodec
import com.github.ldaniels528.trifecta.io.json.JsonHelper._
import com.github.ldaniels528.trifecta.io.mongodb.{MongoOutputSource, TxMongoCluster, TxMongoDB}
import com.github.ldaniels528.trifecta.io.{InputSource, KeyAndMessage}
import com.github.ldaniels528.trifecta.messages.{BinaryMessaging, MessageDecoder}
import com.github.ldaniels528.trifecta.util.ParsingHelper._
import com.github.ldaniels528.trifecta.{TxConfig, TxRuntimeContext}
import com.github.ldaniels528.commons.helpers.ResourceHelper._
import com.github.ldaniels528.commons.helpers.StringHelper._
import com.mongodb.WriteResult
import net.liftweb.json.JsonAST.JValue
import net.liftweb.json.compactRender

import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.Try

/**
  * MongoDB Module
  * @author lawrence.daniels@gmail.com
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
    Command(this, "mfindone", findDocument, UnixLikeParams(Seq("collection" -> true, "query" -> false, "fields" -> false), Seq("-o" -> "outputSource")), help = "Retrieves a document from MongoDB"),
    Command(this, "mfind", findDocuments, UnixLikeParams(Seq("collection" -> true, "query" -> false, "fields" -> false), Seq("-o" -> "outputSource")), help = "Retrieves a document from MongoDB"),
    Command(this, "mget", findDocument, UnixLikeParams(Seq("collection" -> true, "query" -> true, "fields" -> false), Seq("-o" -> "outputSource")), help = "Retrieves a document from MongoDB"),
    Command(this, "mput", insertDocument, UnixLikeParams(Seq("collection" -> true, "json" -> true)), help = "Inserts a document into MongoDB"),
    Command(this, "use", selectDatabase, UnixLikeParams(Seq("database" -> true)), help = "Sets the current MongoDB database")
  )

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
  override def supportedPrefixes = Seq("mongo")

  /**
    * Returns the label of the module (e.g. "kafka")
    * @return the label of the module
    */
  override def moduleLabel = "mongo"

  /**
    * Returns the name of the module (e.g. "kafka")
    * @return the name of the module
    */
  override def moduleName = "mongodb"

  override def prompt = database_?.map(_.databaseName).getOrElse("[default]")

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
    Try(cluster_?.foreach(_.close()))
    cluster_? = Option(TxMongoCluster(hosts))
  }

  /**
    * Retrieves a MongoDB document
    * @example mget Stocks { "symbol" : "AAPL" }
    * @example mget Stocks { "symbol" : "AAPL" } { "symbol":true, "exchange":true, "lastTrade":true }
    * @example mget Stocks { "symbol" : "AAPL" } -o es:/quotes/quote/AAPL
    */
  def findDocument(params: UnixLikeArgs)(implicit rt: TxRuntimeContext): Option[JValue] = {
    // retrieve the document from the collection
    val document = params.args match {
      case List(tableName) => database.getCollection(tableName).findOne()
      case List(tableName, query) => database.getCollection(tableName).findOne(toJson(query))
      case List(tableName, query, fields) => database.getCollection(tableName).findOne(toJson(query), toJson(fields))
      case _ => dieSyntax(params)
    }

    // determine which decoder to use; either the user specified decoder, cursor's decoder or none
    val decoder: Option[MessageDecoder[_]] = params("-a") map AvroCodec.resolve

    // write the document to an output source?
    for {
      out <- getOutputSource(params)
      doc <- document
    } {
      val key = hexToBytes((doc \ "_id" \ "$oid").values.toString)
      val value = compactRender(doc).getBytes(config.encoding)
      out use (_.write(KeyAndMessage(key, value), decoder))
    }

    document
  }

  /**
    * Retrieves a list of MongoDB documents
    * @example mfind Stocks { "symbol" : "AAPL" } -o es:/quotes/quote/AAPL
    * @example mfind Stocks { } -o topic:com.shocktrade.stocks.avro -a file:avro/quotes.avsc
    * @example mfind Stocks { "symbol" : "AAPL" } { "symbol":true, "exchange":true, "lastTrade":true } -o es:/quotes/quote/AAPL
    */
  def findDocuments(params: UnixLikeArgs)(implicit rt: TxRuntimeContext) = {
    // retrieve the documents from the collection
    val documents = params.args match {
      case List(tableName) => database.getCollection(tableName).find()
      case List(tableName, query) => database.getCollection(tableName).find(toJson(query))
      case List(tableName, query, fields) => database.getCollection(tableName).find(toJson(query), toJson(fields))
      case _ => dieSyntax(params)
    }

    // determine which decoder to use; either the user specified decoder, cursor's decoder or none
    val decoder = params("-a") map AvroCodec.resolve

    // write the document to an output source?
    val outputSource_? = getOutputSource(params)
    for {
      out <- outputSource_?
      _ = out.open()
      doc <- documents
    } {
      val key = hexToBytes((doc \ "_id" \ "$oid").values.toString)
      val value = compactRender(doc).getBytes(config.encoding)
      out.write(KeyAndMessage(key, value), decoder)
    }
    outputSource_?.foreach(_.close())

    documents
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
