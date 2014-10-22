package com.ldaniels528.trifecta.support.zookeeper

import java.nio.ByteBuffer
import java.util

import com.ldaniels528.trifecta.support.zookeeper.ZKProxy.Implicits._
import com.ldaniels528.trifecta.support.zookeeper.ZKProxyV1._
import org.apache.zookeeper.CreateMode._
import org.apache.zookeeper.ZooDefs.Ids
import org.apache.zookeeper.ZooKeeper.States
import org.apache.zookeeper._
import org.apache.zookeeper.data.{ACL, Stat}
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._
import scala.language.implicitConversions
import scala.util.Try

/**
 * ZooKeeper Proxy (Version 1.0)
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class ZKProxyV1(val host: String, val port: Int, callback: Option[ZkProxyCallBack] = None) extends ZKProxy {
  private lazy val logger = LoggerFactory.getLogger(getClass)
  var acl: util.ArrayList[ACL] = Ids.OPEN_ACL_UNSAFE
  var mode: CreateMode = PERSISTENT
  var encoding: String = "UTF8"

  logger.info(s"Connecting to ZooKeeper at '$host:$port'...")
  private var zk = new ZooKeeper(s"$host:$port", 5000, new MyWatcher(callback))

  def batch(ops: Op*): Seq[OpResult] = ??? // zk.multi(ops)

  def client: ZooKeeper = zk

  override def close(): Unit = zk.close()

  override def create(tuples: (String, Array[Byte])*): Iterable[String] = {
    tuples map {
      case (node, data) =>
        zk.create(node, data, acl, mode)
    }
  }

  override def create(path: String, data: Array[Byte]): String = zk.create(path, data, acl, mode)

  override def createDirectory(path: String): String = zk.create(path, NO_DATA, acl, mode)

  override def delete(path: String) = exists_?(path) foreach (stat => zk.delete(path, stat.getVersion))

  def delete(path: String, stat: Stat) = zk.delete(path, stat.getVersion)

  override def ensurePath(path: String): List[String] = {
    val items = path.splitNodes map (p => (p, NO_DATA))
    items flatMap {
      case (node, data) => Option(zk.create(node, data, acl, mode))
    }
  }

  override def ensureParents(path: String): List[String] = {
    val items = path.splitNodes.init map (p => (p, NO_DATA))
    items flatMap {
      case (node, data) => Option(zk.create(node, data, acl, mode))
    }
  }

  override def exists(path: String): Boolean = Option(zk.exists(path, false)).isDefined

  override def exists_?(path: String, watch: Boolean = false): Option[Stat] = Option(zk.exists(path, watch))

  override def getChildren(path: String, watch: Boolean = false): Seq[String] = zk.getChildren(path, watch)

  override def getCreationTime(path: String): Option[Long] = {
    Option(zk.exists(path, false)) map(_.getCtime)
  }

  override def getFamily(path: String): List[String] = {

    def zkKeyToPath(parent: String, child: String): String = (if (parent.endsWith("/")) parent else parent + "/") + child

    val children = Option(getChildren(path)) getOrElse Seq.empty
    path :: (children flatMap (child => getFamily(zkKeyToPath(path, child)))).toList
  }

  def getSessionId: Long = zk.getSessionId

  def getState: States = zk.getState

  def read(path: String, stat: Stat): Option[Array[Byte]] = Option(zk.getData(path, false, stat))

  override def read(path: String): Option[Array[Byte]] = exists_?(path) flatMap (stat => Option(zk.getData(path, false, stat)))

  def readDouble(path: String, stat: Stat): Option[Double] = read(path, stat) map ByteBuffer.wrap map (_.getDouble)

  override def readDouble(path: String): Option[Double] = read(path) map ByteBuffer.wrap map (_.getDouble)

  def readInt(path: String, stat: Stat): Option[Int] = read(path, stat) map ByteBuffer.wrap map (_.getInt)

  override def readInt(path: String): Option[Int] = read(path) map ByteBuffer.wrap map (_.getInt)

  def readLong(path: String, stat: Stat): Option[Long] = read(path, stat) map ByteBuffer.wrap map (_.getLong)

  override def readLong(path: String): Option[Long] = read(path) map ByteBuffer.wrap map (_.getLong)

  def readString(path: String, stat: Stat): Option[String] = read(path, stat) map (new String(_, encoding))

  override def readString(path: String): Option[String] = read(path) map (new String(_, encoding))

  override def reconnect() {
    Try(zk.close())
    zk = new ZooKeeper(host, port, new MyWatcher(callback))
  }

  override def remoteHost = s"$host:$port"

  /**
   * Updates the given path
   */
  def updateAtomic(path: String, data: Array[Byte], stat: Stat): Seq[OpResult] = {
    batch(
      Op.delete(path, stat.getVersion),
      Op.create(path, data, acl, mode))
  }

  override def update(path: String, data: Array[Byte]): Iterable[String] = {
    delete(path)
    create(path -> data)
  }

  override def updateLong(path: String, value: Long): Iterable[String] = {
    // write the value to a byte array
    val data = new Array[Byte](8)
    ByteBuffer.wrap(data).putLong(value)

    // perform the update
    update(path, data)
  }

}

/**
 * Zookeeper Proxy Singleton
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
object ZKProxyV1 {
  private val NO_DATA = new Array[Byte](0)

  /**
   * My ZooKeeper Watcher Callback
   * @param callback the [[ZkProxyCallBack]]
   */
  class MyWatcher(callback: Option[ZkProxyCallBack]) extends Watcher {
    override def process(event: WatchedEvent) = callback.foreach(_.process(event))
  }

}

