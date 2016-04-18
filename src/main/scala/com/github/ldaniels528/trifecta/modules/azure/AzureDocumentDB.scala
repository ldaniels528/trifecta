package com.github.ldaniels528.trifecta.modules.azure

import com.github.ldaniels528.trifecta.TxResultHandler.Ok
import com.github.ldaniels528.trifecta.command.UnixLikeArgs
import com.github.ldaniels528.trifecta.io.KeyAndMessage
import com.github.ldaniels528.trifecta.io.avro.AvroCodec
import com.github.ldaniels528.trifecta.io.json.JsonHelper._
import com.github.ldaniels528.trifecta.messages.BinaryMessaging
import com.github.ldaniels528.trifecta.modules.ModuleCommandAgent
import com.github.ldaniels528.trifecta.{TxConfig, TxRuntimeContext}
import com.microsoft.azure.documentdb.Document

import scala.collection.JavaConversions._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.language.postfixOps

/**
  * Azure DocumentDB Module
  * @author lawrence.daniels@gmail.com
  */
case class AzureDocumentDB(module: AzureModule, config: TxConfig, connection: TxDocumentDbConnection)
  extends ModuleCommandAgent with BinaryMessaging {
  private var cursor_? : Option[java.util.Iterator[Document]] = None

  /**
    * Closes the database connection
    */
  def close() = connection.close()

  /**
    * Retrieves a list of DocumentDB documents
    * @param params the given [[UnixLikeArgs arguments]]
    */
  def findDocuments(params: UnixLikeArgs)(implicit rt: TxRuntimeContext) = {
    // retrieve the documents from the collection
    val response = params.args match {
      case List(query) => connection.queryDocuments(query)
      case _ => dieSyntax(params)
    }

    // extract the documents
    cursor_? = Option(response.getQueryIterable.iterator())

    // determine which decoder to use; either the user specified decoder, cursor's decoder or none
    val decoder = params("-a") map AvroCodec.resolve

    // write the document to an output source?
    val outputSource_? = module.getOutputSource(params)
    for {
      out <- outputSource_?
      _ = out.open()
      doc <- response.getQueryIterable.iterator()
    } {
      val key = doc.getId.getBytes(config.encoding)
      val value = doc.toString.getBytes(config.encoding)
      out.write(KeyAndMessage(key, value), decoder)
    }
    outputSource_?.foreach(_.close())

    nextMessage
  }

  /**
    * Optionally returns the next message
    * @param params the given [[UnixLikeArgs arguments]]
    */
  def getNextMessage(params: UnixLikeArgs)(implicit rt: TxRuntimeContext) = {
    params.args match {
      case Nil => nextMessage
      case _ => dieSyntax(params)
    }
  }

  /**
    * Inserts a document into the database
    * @param params the given [[UnixLikeArgs arguments]]
    */
  def insertDocument(params: UnixLikeArgs) = {
    val jsonString = params.args match {
      case List(aJsonString) => compressJson(aJsonString)
      case _ => dieSyntax(params)
    }

    // insert the document into the collection
    connection.createDocument(jsonString)
    Ok()
  }

  /**
    * Returns the the information that is to be displayed while the module is active
    * @return the the information that is to be displayed while the module is active
    */
  def prompt = s"${connection.databaseName}.${connection.collectionName}"

  /**
    * Returns the next document from the cursor
    * @return the next document from the cursor
    */
  private def nextMessage = cursor_? flatMap { it =>
    if (it.hasNext) Option(toJson(it.next().toString)) else None
  }

}
