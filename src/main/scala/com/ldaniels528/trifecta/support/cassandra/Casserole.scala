package com.ldaniels528.trifecta.support.cassandra

import java.util.concurrent.{ExecutorService, Executors}

import com.datastax.driver.core.Cluster

/**
 * Casserole: DataStax/Cassandra Scala Client
 * @author lawrence.daniels@gmail.com
 */
class Casserole(cluster: Cluster, threadPool: ExecutorService) {

  /**
   * Closes the connection
   */
  def close(): Unit = cluster.close()

  /**
   * Creates a new session for the given key space
   * @param keySpace the given key space
   * @return a new [[CasseroleSession]]
   */
  def getSession(keySpace: String): CasseroleSession = new CasseroleSession(cluster.connect(keySpace), threadPool)

}

/**
 * Casserole Singleton
 * @author lawrence.daniels@gmail.com
 */
object Casserole {

  /**
   * Opens a connection to the cluster representing by the given servers
   * @param servers the given servers
   * @return a connection to the cluster representing by the given servers
   */
  def apply(servers: Seq[String]): Casserole = {
    val cluster = servers.foldLeft[Cluster.Builder](new Cluster.Builder()) { case (builder, server) =>
      builder.addContactPoint(server)
      builder
    }.build()
    new Casserole(cluster, Executors.newCachedThreadPool())
  }

}