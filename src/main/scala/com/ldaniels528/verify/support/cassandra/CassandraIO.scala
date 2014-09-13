package com.ldaniels528.verify.support.cassandra

import com.datastax.driver.core.{BoundStatement, Cluster, ConsistencyLevel, PreparedStatement, ResultSet, Session}
import com.ldaniels528.verify.util.EndPoint

import scala.collection.concurrent.TrieMap
import scala.concurrent.{Future, Promise}
import scala.util.Try

/**
 * DataStax/Cassandra Data Access Object Implementation
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class CassandraIO(keySpaceName: String, hosts: EndPoint*) {
  val psCache = new TrieMap[String, PreparedStatement]()
  private val threadPool = java.util.concurrent.Executors.newCachedThreadPool()

  def getConnection: Cluster = {
    hosts.foldLeft[Cluster.Builder](new Cluster.Builder()) { (builder, host) =>
      builder.addContactPoint(host.toString)
    }.build()
  }

  def getSession(implicit cluster: Cluster): Session = {
    cluster.connect(keySpaceName)
  }

  def executeAsync(cql: String)(implicit session: Session): Future[Unit] = {
    // execute the query
    val futureResult = session.executeAsync(cql)

    // create a promise of a result
    val promise = Promise[Unit]()
    futureResult.addListener(new Runnable {
      override def run() {
        promise.complete(Try(()))
        ()
      }
    }, threadPool)

    // return the future
    promise.future
  }

  def insert(table: String, _props: Map[String, Any], cl: ConsistencyLevel)(implicit session: Session): ResultSet = {
    //props foreach { case (k,v) => System.out.println(s"$k = $v (${if(v != null) v.getClass.getName else "<null>"})") }
    //System.out.println("*"*40)
    val props = _props.get("active") match {
      case Some(v: String) => _props + ("active" -> v.equals("true"))
      case _ => _props
    }

    // create the query
    val fields = props.keys.toSeq
    val cql = s"INSERT INTO $table (${fields mkString ","}) VALUES (${fields map (s => "?") mkString ","})"

    // lookup the prepared statement
    val ps = getPreparedStatement(cql, cl)

    // create & populate the bound statement
    val bs = new BoundStatement(ps)
    val values = fields map (f => props.get(f).orNull.asInstanceOf[Object])
    bs.bind(values: _*)

    // execute the statement
    session.execute(bs)
  }

  def insert[T](table: String, bean: T, cl: ConsistencyLevel)(implicit session: Session): ResultSet = {
    insert(table, Cascade.mapify(bean), cl)
  }

  private def getPreparedStatement(cql: String, cl: ConsistencyLevel)(implicit session: Session): PreparedStatement = {
    psCache.get(cql) match {
      case Some(ps) => ps
      case None =>
        val ps = session.prepare(cql)
        ps.setConsistencyLevel(cl)
        psCache.putIfAbsent(cql, ps)
        ps
    }
  }

}