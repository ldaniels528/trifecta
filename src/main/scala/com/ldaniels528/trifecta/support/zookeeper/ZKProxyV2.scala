package com.ldaniels528.trifecta.support.zookeeper

import com.ldaniels528.trifecta.support.zookeeper.ZKProxy.Implicits._
import org.I0Itec.zkclient.ZkClient
import org.apache.zookeeper.CreateMode
import org.apache.zookeeper.CreateMode._
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._
import scala.language.implicitConversions
import scala.util.Try

/**
 * ZooKeeper Proxy (Version 2.0)
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
case class ZKProxyV2(connectionString: String) extends ZKProxy {
  private val logger = LoggerFactory.getLogger(getClass)
  private val NO_DATA = new Array[Byte](0)

  private var zk = new ZkClient(connectionString)

  var mode: CreateMode = PERSISTENT
  var encoding: String = "UTF-8"

  def client: ZkClient = zk

  override def close(): Unit = zk.close()

  override def create(path: String, data: Array[Byte]) = zk.create(path, data, CreateMode.PERSISTENT)

  override def create(tupleSeq: (String, Array[Byte])*): Iterable[String] = {
    tupleSeq.map { case (path, data) => create(path, data)}
  }

  override def createDirectory(path: String): String = zk.create(path, NO_DATA, mode)

  override def delete(path: String) = zk.delete(path)

  override def deleteRecursive(path: String) = zk.deleteRecursive(path)

  override def exists(path: String): Boolean = zk.exists(path)

  override def getChildren(path: String, watch: Boolean): Seq[String] = zk.getChildren(path).toSeq

  override def getCreationTime(path: String): Option[Long] = Option(zk.getCreationTime(path))

  override def read(path: String): Option[Array[Byte]] = Option(zk.readData(path))

  override def readString(path: String): Option[String] = Option(zk.readData[String](path, true))

  override def reconnect() {
    Try(zk.close())
    zk = new ZkClient(connectionString)
  }

  override def update(path: String, data: Array[Byte]): Iterable[String] = {
    delete(path)
    create(path -> data)
  }

  override def ensurePath(path: String): List[String] = {
    val items = path.splitNodes map (p => (p, NO_DATA))
    items flatMap {
      case (node, data) => Option(zk.create(node, data, mode))
    }
  }

  override def ensureParents(path: String): List[String] = {
    val items = path.splitNodes.init map (p => (p, NO_DATA))
    items flatMap {
      case (node, data) => Option(zk.create(node, data, mode))
    }
  }

  override def getFamily(path: String): List[String] = {

    def zkKeyToPath(parent: String, child: String): String = (if (parent.endsWith("/")) parent else parent + "/") + child

    val children = Option(getChildren(path)) getOrElse Seq.empty
    path :: (children flatMap (child => getFamily(zkKeyToPath(path, child)))).toList
  }

  override def remoteHost: String = connectionString

}
