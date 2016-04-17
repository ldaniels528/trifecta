package com.github.ldaniels528.trifecta.io.zookeeper

import org.apache.curator.RetryPolicy
import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.zookeeper.CreateMode
import org.apache.zookeeper.CreateMode._

import scala.collection.JavaConversions._

/**
 * ZooKeeper Proxy (Apache Curator client)
 * @author lawrence.daniels@gmail.com
 */
case class ZkProxyCurator(connectionString: String) extends ZKProxy {
  var mode: CreateMode = PERSISTENT
  var encoding: String = "UTF-8"
  var retryPolicy: RetryPolicy = new ExponentialBackoffRetry(1000, 3)
  var client: CuratorFramework = _

  // create the connection
  connect()

  override def close(): Unit = client.close()

  override def connect(): Unit = {
    client = CuratorFrameworkFactory.newClient(connectionString, retryPolicy)
    client.start()
  }

  override def create(tupleSeq: (String, Array[Byte])*): Iterable[String] = {
    tupleSeq.map { case (path, bytes) =>
      client.create().creatingParentsIfNeeded().forPath(path, bytes)
    }
  }

  override def create(path: String, bytes: Array[Byte]): String = {
    client.create().creatingParentsIfNeeded().forPath(path, bytes)
  }

  override def createDirectory(path: String): String = {
    client.create().creatingParentsIfNeeded().forPath(path)
  }

  override def delete(path: String): Boolean = {
    client.delete().forPath(path)
    exists(path)
  }

  override def deleteRecursive(path: String): Boolean = {
    client.delete().deletingChildrenIfNeeded().forPath(path)
    exists(path)
  }

  override def exists(path: String): Boolean = {
    Option(client.checkExists().forPath(path)).isDefined
  }

  override def ensurePath(path: String): List[String] = {
    List(client.create().creatingParentsIfNeeded().forPath(path)) // TODO
  }

  override def ensureParents(path: String): List[String] = {
    List(client.create().creatingParentsIfNeeded().forPath(path)) // TODO
  }

  override def getChildren(path: String, watch: Boolean): Seq[String] = {
    client.getChildren.forPath(path).toSeq
  }

  override def getCreationTime(path: String): Option[Long] = {
    Option(client.checkExists().forPath(path)) map (_.getCtime)
  }

  override def getModificationTime(path: String): Option[Long] = {
    Option(client.checkExists().forPath(path)) map (_.getMtime)
  }

  override def getFamily(path: String): List[String] = {

    def zkKeyToPath(parent: String, child: String): String = (if (parent.endsWith("/")) parent else parent + "/") + child

    val children = Option(getChildren(path)) getOrElse Nil
    path :: (children flatMap (child => getFamily(zkKeyToPath(path, child)))).toList
  }

  override def read(path: String): Option[Array[Byte]] = Option(client.getData.forPath(path))

  override def readString(path: String): Option[String] = {
    Option(client.getData.forPath(path)) map (new String(_, encoding))
  }

  override def update(path: String, bytes: Array[Byte]): Option[String] = {
    Option(client.checkExists().forPath(path)) map { stat =>
      client.delete().forPath(path)
      client.create().forPath(path, bytes)
    }
  }

}
