package com.github.ldaniels528.trifecta.modules.etl.io.device.impl

import java.sql.{Connection, DriverManager, PreparedStatement, Timestamp}
import java.util.{Date, UUID}

import com.github.ldaniels528.trifecta.modules.etl.io.Scope
import com.github.ldaniels528.trifecta.modules.etl.io.device.impl.SQLOutputSource._
import com.github.ldaniels528.trifecta.modules.etl.io.device.{DataSet, OutputSource}
import com.github.ldaniels528.trifecta.modules.etl.io.layout.Layout
import com.github.ldaniels528.trifecta.modules.etl.io.record.DataTypes._
import com.github.ldaniels528.trifecta.modules.etl.io.record.{Field, Record}
import com.github.ldaniels528.commons.helpers.OptionHelper._
import com.github.ldaniels528.trifecta.util.ResourcePool
import org.slf4j.LoggerFactory

import scala.collection.concurrent.TrieMap

/**
  * SQL Output Source
  * @author lawrence.daniels@gmail.com
  */
case class SQLOutputSource(id: String, connectionInfo: SQLConnectionInfo, layout: Layout) extends OutputSource {
  private val sqlCache = TrieMap[String, SQLStatement]()
  private val uuid = UUID.randomUUID()
  private var tableName: String = _

  override def close(implicit scope: Scope) = {
    scope.discardResource[Connection](uuid).foreach(_.close())
  }

  override def open(implicit scope: Scope) = {
    sqlCache.clear()
    scope ++= Seq(
      "flow.output.id" -> id,
      "flow.output.count" -> (() => getStatistics.count),
      "flow.output.offset" -> (() => getStatistics.offset)
    )
    scope.createResource(uuid, connectionInfo.connect())
    tableName = connectionInfo.getTable
    ()
  }

  override def writeRecord(record: Record, dataSet: DataSet)(implicit scope: Scope) = {
    scope.getResource[Connection](uuid) map { conn =>
      val sql = sqlCache.getOrElseUpdate(s"${tableName}_INSERT", SQLInsert(conn, record, tableName))
      updateCount(sql.execute())
    } orDie s"SQL output source '$id' has not been opened"
  }

}

/**
  * SQL Output Source Companion Object
  * @author lawrence.daniels@gmail.com
  */
object SQLOutputSource {
  private[this] val logger = LoggerFactory.getLogger(getClass)
  private val sqlTypeMapping = Map(
    BINARY -> java.sql.Types.VARBINARY,
    BOOLEAN -> java.sql.Types.BOOLEAN,
    DOUBLE -> java.sql.Types.DOUBLE,
    DATE -> java.sql.Types.DATE,
    FLOAT -> java.sql.Types.FLOAT,
    INT -> java.sql.Types.INTEGER,
    LONG -> java.sql.Types.BIGINT,
    STRING -> java.sql.Types.VARCHAR
  )

  private def getSQLType(`type`: DataType) = {
    sqlTypeMapping.get(`type`) orDie s"Unhandled data type - ${`type`}"
  }

  /**
    * SQL Connection Information
    * @param driver   the JDBC driver class (e.g. "com.microsoft.sqlserver.jdbc.SQLServerDriver")
    * @param url      the JBDC URL (e.g. "jdbc:sqlserver://ladaniel.database.windows.net:1433;database=ladaniel_sql")
    * @param user     the JDBC user name
    * @param password the JDBC user password
    * @see [[https://azure.microsoft.com/en-us/documentation/articles/sql-database-develop-java-simple-windows/]]
    */
  case class SQLConnectionInfo(driver: String, url: String, user: String, password: String, table: String) {

    def connect()(implicit scope: Scope) = {
      Class.forName(scope.evaluateAsString(driver))
      DriverManager.getConnection(scope.evaluateAsString(url), scope.evaluateAsString(user), scope.evaluateAsString(password))
    }

    def getTable(implicit scope: Scope) = scope.evaluateAsString(table)

  }

  /**
    * Represents an executable SQL statement
    */
  trait SQLStatement {

    def execute()(implicit scope: Scope): Int

    protected def populateAndExecute(ps: PreparedStatement, fields: Seq[Field])(implicit scope: Scope) = {
      // populate the prepared statement
      fields zip fields.indices.map(_ + 1) foreach { case (field, index) =>
        field.value match {
          case Some(value) =>
            value match {
              case date: Date => ps.setTimestamp(index, new Timestamp(date.getTime))
              case v => ps.setObject(index, v)
            }

          case None => ps.setNull(index, getSQLType(field.`type`))
        }
      }

      // perform the update
      ps.executeUpdate()
    }

  }

  /**
    * Represents a SQL INSERT statement
    * @param conn   the given [[Connection JDBC connection]]
    * @param record the given [[Record record]]
    */
  case class SQLInsert(conn: Connection, record: Record, tableName: String) extends SQLStatement {
    private val query = generateQuery()
    private val psCache = ResourcePool[PreparedStatement](() => conn.prepareStatement(query))

    override def execute()(implicit scope: Scope) = {
      val ps = psCache.take
      try populateAndExecute(ps, record.fields) finally psCache.give(ps)
    }

    override def toString = query

    private def generateQuery() = {
      val columns = record.fields.map(_.name).mkString(", ")
      val values = record.fields.map(_ => "?").mkString(", ")
      val sql = s"INSERT INTO $tableName ($columns) VALUES ($values)"
      logger.info(s"SQL: $sql")
      sql
    }
  }

  /**
    * Represents a SQL UPDATE statement
    * @param conn   the given [[Connection JDBC connection]]
    * @param record the given [[Record record]]
    */
  case class SQLUpdate(conn: Connection, record: Record, tableName: String) extends SQLStatement {
    private val query = generateQuery()
    private val psCache = ResourcePool[PreparedStatement](() => conn.prepareStatement(query))
    private val fields = record.fields ++ record.fields.filter(_.updateKey.contains(true))

    override def execute()(implicit scope: Scope) = {
      val ps = psCache.take
      try populateAndExecute(ps, fields) finally psCache.give(ps)
    }

    override def toString = query

    private def generateQuery() = {
      val pairs = record.fields.map(f => s"${f.name} = ?").mkString(", ")
      val condition = record.fields.filter(_.updateKey.contains(true)).map(f => s"${f.name} = ?").mkString(" AND ")
      if (condition.isEmpty)
        throw new IllegalStateException("A SQL update is not possible without specifying update fields")
      val sql = s"UPDATE $tableName SET $pairs WHERE $condition"
      logger.info(s"SQL: $sql")
      sql
    }
  }

}
