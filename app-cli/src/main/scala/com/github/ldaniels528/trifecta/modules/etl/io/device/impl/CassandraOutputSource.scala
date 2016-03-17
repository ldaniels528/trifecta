package com.github.ldaniels528.trifecta.modules.etl.io.device.impl

import java.util.UUID

import com.datastax.driver.core.{BoundStatement, Cluster, PreparedStatement, Session}
import com.github.ldaniels528.trifecta.modules.etl.io.Scope
import com.github.ldaniels528.trifecta.modules.etl.io.device.impl.CassandraOutputSource.{ConnectionInfo, _}
import com.github.ldaniels528.trifecta.modules.etl.io.device.{DataSet, OutputSource}
import com.github.ldaniels528.trifecta.modules.etl.io.layout.Layout
import com.github.ldaniels528.trifecta.modules.etl.io.record.{Field, Record}
import com.github.ldaniels528.commons.helpers.OptionHelper._
import org.slf4j.LoggerFactory

import scala.collection.concurrent.TrieMap

/**
  * Cassandra Output Source
  * @author lawrence.daniels@gmail.com
  */
case class CassandraOutputSource(id: String, connectionInfo: ConnectionInfo, layout: Layout) extends OutputSource {
  private val sqlCache = TrieMap[String, CQLStatement]()
  private val uuid = UUID.randomUUID()
  private var tableName: String = _

  override def close(implicit scope: Scope) = {
    scope.discardResource[ConnectionState](uuid).foreach(_._1.close())
  }

  override def open(implicit scope: Scope) = {
    scope ++= Seq(
      "flow.output.id" -> id,
      "flow.output.count" -> (() => getStatistics.count),
      "flow.output.offset" -> (() => getStatistics.offset)
    )
    scope.createResource(uuid, connectionInfo.connect())
    tableName = connectionInfo.getTable
  }

  override def writeRecord(record: Record, dataSet: DataSet)(implicit scope: Scope) = {
    scope.getResource[ConnectionState](uuid) map { case (_, session) =>
      val sql = sqlCache.getOrElseUpdate(s"${tableName}_INSERT", CQLInsert(session, record, tableName))
      updateCount(sql.execute())
    } orDie s"Cassandra output source '$id' has not been opened"
  }

}

/**
  * Cassandra Output Source Companion Object
  * @author lawrence.daniels@gmail.com
  */
object CassandraOutputSource {
  private[this] val logger = LoggerFactory.getLogger(getClass)

  type ConnectionState = (Cluster, Session)

  /**
    * Cassandra Connection Information
    * @param keySpace the given key space
    * @param table    the given table
    * @param servers  the given list of servers
    */
  case class ConnectionInfo(keySpace: String, table: String, servers: Seq[String]) {

    def connect()(implicit scope: Scope) = {
      val cluster = servers.foldLeft[Cluster.Builder](new Cluster.Builder()) { case (builder, server) =>
        builder.addContactPoint(server)
        builder
      }.build()
      (cluster, cluster.connect(getKeySpace))
    }

    def getKeySpace(implicit scope: Scope) = scope.evaluateAsString(keySpace)

    def getTable(implicit scope: Scope) = scope.evaluateAsString(table)

  }

  /**
    * Represents an executable CQL statement
    */
  trait CQLStatement {

    def execute()(implicit scope: Scope): Int

    protected def populateAndExecute(session: Session, ps: PreparedStatement, fields: Seq[Field])(implicit scope: Scope) = {
      val values = fields.map(_.value: Object)
      val bs = new BoundStatement(ps).bind(values)
      val rs = session.execute(bs)
      Option(rs.one()) foreach { row =>
        logger.info(s"row = $row")
      }
      1
    }

  }

  /**
    * Represents a CQL INSERT statement
    * @param session the given [[Session session]]
    * @param record  the given [[Record record]]
    */
  case class CQLInsert(session: Session, record: Record, tableName: String) extends CQLStatement {
    private val query = generateQuery()

    override def execute()(implicit scope: Scope) = {
      populateAndExecute(session, session.prepare(query), record.fields)
    }

    override def toString = query

    private def generateQuery() = {
      val columns = record.fields.map(_.name).mkString(", ")
      val values = record.fields.map(_ => "?").mkString(", ")
      val cql = s"INSERT INTO $tableName ($columns) VALUES ($values)"
      logger.info(s"CQL: $cql")
      cql
    }
  }

  /**
    * Represents a CQL UPDATE statement
    * @param session the given [[Session session]]
    * @param record  the given [[Record record]]
    */
  case class CQLUpdate(session: Session, record: Record, tableName: String) extends CQLStatement {
    private val query = generateQuery()
    private val fields = record.fields ++ record.fields.filter(_.updateKey.contains(true))

    override def execute()(implicit scope: Scope) = {
      populateAndExecute(session, session.prepare(query), fields)
    }

    override def toString = query

    private def generateQuery() = {
      val pairs = record.fields.map(f => s"${f.name} = ?").mkString(", ")
      val condition = record.fields.filter(_.updateKey.contains(true)).map(f => s"${f.name} = ?").mkString(" AND ")
      if (condition.isEmpty)
        throw new IllegalStateException("A CQL update is not possible without specifying update fields")
      val cql = s"UPDATE $tableName SET $pairs WHERE $condition"
      logger.info(s"CQL: $cql")
      cql
    }
  }

}