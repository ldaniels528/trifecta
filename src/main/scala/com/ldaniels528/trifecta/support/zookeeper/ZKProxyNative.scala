package com.ldaniels528.trifecta.support.zookeeper

import java.util

import com.ldaniels528.trifecta.support.zookeeper.ZKProxy.Implicits._
import com.ldaniels528.trifecta.support.zookeeper.ZKProxyNative._
import com.ldaniels528.trifecta.support.zookeeper.ZkSupportHelper._
import org.apache.zookeeper.CreateMode._
import org.apache.zookeeper.ZooDefs.Ids
import org.apache.zookeeper._
import org.apache.zookeeper.data.{ACL, Stat}
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._
import scala.language.implicitConversions
import scala.util.Try

/**
 * ZooKeeper Proxy (Apache Zookeeper native client)
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
case class ZKProxyNative(connectionString: String, callback: Option[ZkProxyCallBack] = None) extends ZKProxy {
  private lazy val logger = LoggerFactory.getLogger(getClass)
  var acl: util.ArrayList[ACL] = Ids.OPEN_ACL_UNSAFE
  var mode: CreateMode = PERSISTENT
  var encoding: String = "UTF-8"

  logger.info(s"Connecting to ZooKeeper at '$connectionString'...")
  private var zk = new ZooKeeper(connectionString, 5000, new MyWatcher(callback))

  def client: ZooKeeper = zk

  override def close(): Unit = zk.close()

  override def create(tupleSeq: (String, Array[Byte])*): Iterable[String] = {
    tupleSeq map { case (path, bytes) => zk.create(path, bytes, acl, mode)}
  }

  override def create(path: String, data: Array[Byte]): String = zk.create(path, data, acl, mode)

  override def createDirectory(path: String): String = zk.create(path, NO_DATA, acl, mode)

  override def delete(path: String): Boolean = {
    exists_?(path) exists { stat =>
      zk.delete(path, stat.getVersion)
      true
    }
  }

  def delete(path: String, stat: Stat) = zk.delete(path, stat.getVersion)

  override def deleteRecursive(path: String): Boolean = {
    val outcomes = zk.getChildren(path, false) map (subPath => deleteRecursive(zkKeyToPath(path, subPath)))
    delete(path) && outcomes.forall(_ == true)
  }

  override def ensureParents(path: String): List[String] = {
    val items = path.splitNodes.init map (p => (p, NO_DATA))
    items flatMap {
      case (node, data) => Option(zk.create(node, data, acl, mode))
    }
  }

  override def ensurePath(path: String): List[String] = {
    val items = path.splitNodes map (p => (p, NO_DATA))
    items flatMap {
      case (node, data) => Option(zk.create(node, data, acl, mode))
    }
  }

  override def exists(path: String): Boolean = Option(zk.exists(path, false)).isDefined

  override def getChildren(path: String, watch: Boolean = false): Seq[String] = zk.getChildren(path, watch)

  override def getCreationTime(path: String): Option[Long] = {
    Option(zk.exists(path, false)) map (_.getCtime)
  }

  override def getFamily(path: String): List[String] = {

    def zkKeyToPath(parent: String, child: String): String = (if (parent.endsWith("/")) parent else parent + "/") + child

    val children = Option(getChildren(path)) getOrElse Seq.empty
    path :: (children flatMap (child => getFamily(zkKeyToPath(path, child)))).toList
  }

  override def read(path: String): Option[Array[Byte]] = exists_?(path) flatMap (stat => Option(zk.getData(path, false, stat)))

  override def readString(path: String): Option[String] = read(path) map (new String(_, encoding))

  override def connect() {
    Try(zk.close())
    zk = new ZooKeeper(connectionString, 5000, new MyWatcher(callback))
  }

  override def update(path: String, data: Array[Byte]): Option[String] = {
    exists_?(path, watch = false) map { stat =>
      batch(
        Op.delete(path, stat.getVersion),
        Op.create(path, data, acl, mode))
    }
    None
  }

  private def batch(ops: Op*): Seq[OpResult] = zk.multi(ops)

  private def exists_?(path: String, watch: Boolean = false): Option[Stat] = Option(zk.exists(path, watch))

}

/**
 * Zookeeper Proxy Singleton
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
object ZKProxyNative {
  private val NO_DATA = new Array[Byte](0)

  /**
   * My ZooKeeper Watcher Callback
   * @param callback the [[ZkProxyCallBack]]
   */
  class MyWatcher(callback: Option[ZkProxyCallBack]) extends Watcher {
    override def process(event: WatchedEvent) = callback.foreach(_.process(event))
  }

}

