package com.github.ldaniels528.trifecta.modules.azure

import com.github.ldaniels528.commons.helpers.OptionHelper.Risky._
import com.github.ldaniels528.trifecta.command.UnixLikeArgs
import com.github.ldaniels528.trifecta.modules.ModuleCommandAgent
import com.github.ldaniels528.trifecta.modules.azure.AzureTableStorage.AzureTable
import com.microsoft.azure.storage.CloudStorageAccount
import com.microsoft.azure.storage.table.CloudTable
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._

/**
  * Azure Table Storage
  * @author lawrence.daniels@gmail.com
  * @see {{ https://azure.microsoft.com/en-us/documentation/articles/storage-java-how-to-use-table-storage/ }}
  */
class AzureTableStorage(storageAccount: CloudStorageAccount) extends ModuleCommandAgent {
  private lazy val logger = LoggerFactory.getLogger(getClass)
  private val tableClient = storageAccount.createCloudTableClient()
  private var cloudTable: Option[CloudTable] = None

  /**
    * Closes the connection
    */
  def close(): Unit = ()

  /**
    * Deletes an existing table
    * @param params the given [[UnixLikeArgs arguments]]
    */
  def deleteTable(params: UnixLikeArgs) = {
    // determine the table name
    val table = params.args match {
      case aTableName :: Nil => tableClient.getTableReference(aTableName)
      case Nil => cloudTable getOrElse die("No table selected")
      case _ => dieSyntax(params)
    }

    table.map(_.deleteIfExists())
  }

  /**
    * Lists tables
    * @param params the given [[UnixLikeArgs arguments]]
    */
  def listTables(params: UnixLikeArgs) = {
    // determine the table name
    val tableName = params.args match {
      case aTableName :: Nil => Some(aTableName)
      case Nil => None
      case _ => dieSyntax(params)
    }

    tableClient.listTables() map AzureTable
  }

  /**
    * Selects a table
    * @param params the given [[UnixLikeArgs arguments]]
    */
  def selectTable(params: UnixLikeArgs) = {
    // determine the table name
    val tableName = params.args match {
      case aTableName :: Nil => aTableName
      case _ => dieSyntax(params)
    }

    cloudTable = tableClient.getTableReference(tableName)
    cloudTable.map(_.createIfNotExists())
  }

}

/**
  * Azure Table Storage Companion
  * @author lawrence.daniels@gmail.com
  */
object AzureTableStorage {

  case class AzureTable(name: String)

}