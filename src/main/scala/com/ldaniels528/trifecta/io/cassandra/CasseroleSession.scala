package com.ldaniels528.trifecta.io.cassandra

import java.io.{File, FileOutputStream, PrintWriter}
import java.text.SimpleDateFormat
import java.util.concurrent.ExecutorService

import com.datastax.driver.core._
import com.ldaniels528.trifecta.io.AsyncIO
import com.ldaniels528.commons.helpers.ResourceHelper._
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._
import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.language.postfixOps

/**
 * Casserole Session
 * @author lawrence.daniels@gmail.com
 */
case class CasseroleSession(session: Session, threadPool: ExecutorService) {
  private lazy val logger = LoggerFactory.getLogger(getClass)
  private val psCache = new TrieMap[String, PreparedStatement]()

  /**
   * Closes the session
   */
  def close(): Unit = session.close()

  /**
   * Exports the query results to the given file
   * @param file the given [[File]]
   * @param cql the given CQL query
   * @param values the given query parameters
   * @param cl the given [[ConsistencyLevel]]
   * @param ec the given [[ExecutionContext]]
   * @return the promise of the number of records written
   */
  def export(file: File, cql: String, values: Any*)(implicit cl: ConsistencyLevel, ec: ExecutionContext): AsyncIO = {
    val ps = getPreparedStatement(cql)

    // create & populate the bound statement
    val bs = new BoundStatement(ps)
    bs.bind(values map (_.asInstanceOf[Object]): _*)

    val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ssZ") // 2015-03-06 09:08:03-0800
    AsyncIO { counter =>
      // write the query results to disk
      new PrintWriter(new FileOutputStream(file)) use { out =>
        val rs = session.execute(bs)
        val columns = rs.getColumnDefinitions.asList() map (cf => (cf.getName, cf)) toSeq
        val labels = columns.map(_._1).toSeq

        // write the header row
        out.println(labels map (s => s""""$s"""") mkString ",")

        // write each row of data
        rs.iterator() foreach { row =>
          counter.updateReadCount(1)
          val values = (columns map { case (label, column) =>
            column.getType.getName.toString match {
              case "boolean" => row.getBool(label)
              case "bigint" => row.getLong(label)
              case "char" | "varchar" => Option(row.getString(label)) map (s => s""""$s"""") getOrElse ""
              case "double" => row.getDouble(label)
              case "date" | "timestamp" => Option(row.getDate(label)) map (d => dateFormat.format(d)) getOrElse ""
              case "int" => row.getInt(label)
              case "uuid" | "timeuuid" => row.getUUID(label)
              case unhandledType =>
                logger.warn(s"No mapping found for column ${column.getName} (type $unhandledType, class ${column.getType.asJavaClass.getName})")
                null
            }
          }).toSeq
          out.println(values mkString ",")
          counter.updateWriteCount(1)
        }
      }
    }
  }

  /**
   * Asynchronously executes the given CQL query
   * @param cql the given CQL query
   * @param values the given bound values
   * @param cl the given [[ConsistencyLevel]]
   * @return a promise of a [[ResultSet]]
   */
  def executeQuery(cql: String, values: Any*)(implicit cl: ConsistencyLevel): Future[ResultSet] = {
    val ps = getPreparedStatement(cql)

    // create & populate the bound statement
    val bs = new BoundStatement(ps)
    bs.bind(values map (_.asInstanceOf[Object]): _*)

    // execute the statement
    promiseResult(session.executeAsync(bs))
  }

  /**
   * Inserts values into the given column family
   * @param columnFamily the given column family
   * @param keyValues the given key-value pairs to insert
   * @param cl the given [[ConsistencyLevel]]
   * @return
   */
  def insert[T](columnFamily: String, keyValues: (String, T)*)(implicit cl: ConsistencyLevel): Future[ResultSet] = {
    //props foreach { case (k,v) => System.out.println(s"$k = $v (${if(v != null) v.getClass.getName else "<null>"})") }
    //System.out.println("*"*40)
    val props = Map(keyValues: _*)

    // create the query
    val fields = props.keys.toSeq
    val cql = s"INSERT INTO $columnFamily (${fields mkString ","}) VALUES (${fields map (s => "?") mkString ","})"

    // lookup the prepared statement
    val ps = getPreparedStatement(cql)

    // create & populate the bound statement
    val bs = new BoundStatement(ps)
    val values = fields map (f => props.getOrElse(f, null).asInstanceOf[Object])
    bs.bind(values: _*)

    // execute the statement
    promiseResult(session.executeAsync(bs))
  }

  def insertBean[T](columnFamily: String, bean: T)(implicit cl: ConsistencyLevel): Future[ResultSet] = {
    insert(columnFamily, Cascade.mapify(bean).toSeq: _*)
  }

  def insertCQL(cql: String, values: Any*)(implicit cl: ConsistencyLevel): Future[ResultSet] = {
    // lookup the prepared statement
    val ps = getPreparedStatement(cql)

    // create & populate the bound statement
    val bs = new BoundStatement(ps)
    bs.bind(values map (_.asInstanceOf[Object]): _*)

    // execute the statement
    promiseResult(session.executeAsync(bs))
  }

  private def promiseResult(rsf: ResultSetFuture): Future[ResultSet] = {
    val promise = Promise[ResultSet]()
    rsf.addListener(new Runnable {
      override def run(): Unit = {
        promise.success(rsf.getUninterruptibly)
        ()
      }
    }, threadPool)
    promise.future
  }

  private def getPreparedStatement(cql: String)(implicit cl: ConsistencyLevel): PreparedStatement = {
    psCache.getOrElse(cql, {
      val ps = session.prepare(cql)
      ps.setConsistencyLevel(cl)
      psCache.putIfAbsent(cql, ps)
      ps
    })
  }

}
