package com.ldaniels528.trifecta.io.cassandra

import java.util.concurrent.ExecutorService

import com.datastax.driver.core._

import scala.collection.concurrent.TrieMap
import scala.concurrent.{Future, Promise}

/**
 * Casserole Session
 * @author lawrence.daniels@gmail.com
 */
case class CasseroleSession(session: Session, threadPool: ExecutorService) {
  private val psCache = new TrieMap[String, PreparedStatement]()

  /**
   * Closes the session
   */
  def close(): Unit = session.close()

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
