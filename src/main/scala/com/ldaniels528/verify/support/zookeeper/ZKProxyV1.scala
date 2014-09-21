package com.ldaniels528.verify.support.zookeeper

import java.nio.ByteBuffer
import java.util

import com.ldaniels528.verify.support.zookeeper.ZKProxy.Implicits._
import com.ldaniels528.verify.support.zookeeper.ZKProxyV1._
import org.apache.zookeeper.AsyncCallback.StringCallback
import org.apache.zookeeper.CreateMode._
import org.apache.zookeeper.KeeperException.Code
import org.apache.zookeeper.ZooDefs.Ids
import org.apache.zookeeper.ZooKeeper.States
import org.apache.zookeeper._
import org.apache.zookeeper.data.{ACL, Stat}
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.language.implicitConversions
import scala.util.Try

/**
 * ZooKeeper Proxy (version 1.0)
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class ZKProxyV1(host: String, port: Int, callback: Option[ZkProxyCallBack] = None) extends ZKProxy {
  private lazy val logger = LoggerFactory.getLogger(getClass)
  var acl: util.ArrayList[ACL] = Ids.OPEN_ACL_UNSAFE
  var mode: CreateMode = PERSISTENT
  var encoding: String = "UTF8"

  logger.info(s"Connecting to ZooKeeper at '$host:$port'...")
  private var zk = new ZooKeeper(host, port, new MyWatcher(callback))

  def batch(ops: Op*): Seq[OpResult] = Seq.empty // zk.multi(ops)

  def client: ZooKeeper = zk

  def close(): Unit = zk.close()

  def create(tuples: (String, Array[Byte])*): Iterable[String] = {
    tuples map {
      case (node, data) =>
        zk.create(node, data, acl, mode)
    }
  }

  def create(path: String, data: Array[Byte]): String = zk.create(path, data, acl, mode)

  def createAsync(path: String, data: Array[Byte])(implicit ec: ExecutionContext): Future[Int] = {
    val promise = Promise[Int]()
    val ctx = new Object()
    zk.create(path, data, acl, mode, new StringCallback {
      override def processResult(rc: Int, path: String, ctx: Any, name: String): Unit = {
        if (rc != Code.OK.intValue()) {
          promise.failure(KeeperException.create(KeeperException.Code.get(rc), path))
        } else {
          promise.success(rc)
        }
        ()
      }
    }, ctx)
    promise.future
  }

  def ensurePath(path: String): List[String] = {
    val items = path.splitNodes map (p => (p, NO_DATA))
    items flatMap {
      case (node, data) => Option(zk.create(node, data, acl, mode))
    }
  }

  def ensurePathAsync(path: String, ctx: Any)(implicit ec: ExecutionContext): Future[List[Int]] = {
    val items = path.splitNodes map (p => (p, NO_DATA))
    Future.sequence(items map {
      case (node, data) => createAsync(node, data)
    })
  }

  def ensureParents(path: String): List[String] = {
    val items = path.splitNodes.init map (p => (p, NO_DATA))
    items flatMap {
      case (node, data) => Option(zk.create(node, data, acl, mode))
    }
  }

  def delete(path: String) = exists(path) foreach (stat => zk.delete(path, stat.getVersion))

  def delete(path: String, stat: Stat) = zk.delete(path, stat.getVersion)

  def exists(path: String, watch: Boolean = false): Option[Stat] = Option(zk.exists(path, watch))

  def getChildren(path: String, watch: Boolean = false): Seq[String] = zk.getChildren(path, watch)

  def getFamily(path: String): List[String] = {

    def zkKeyToPath(parent: String, child: String): String = (if (parent.endsWith("/")) parent else parent + "/") + child

    val children = Option(getChildren(path)) getOrElse Seq.empty
    path :: (children flatMap (child => getFamily(zkKeyToPath(path, child)))).toList
  }

  def getSessionId: Long = zk.getSessionId

  def getState: States = zk.getState

  def read(path: String, stat: Stat): Option[Array[Byte]] = Option(zk.getData(path, false, stat))

  def read(path: String): Option[Array[Byte]] = exists(path) flatMap (stat => Option(zk.getData(path, false, stat)))

  def readDouble(path: String, stat: Stat): Option[Double] = read(path, stat) map ByteBuffer.wrap map (_.getDouble)

  def readDouble(path: String): Option[Double] = read(path) map ByteBuffer.wrap map (_.getDouble)

  def readInt(path: String, stat: Stat): Option[Int] = read(path, stat) map ByteBuffer.wrap map (_.getInt)

  def readInt(path: String): Option[Int] = read(path) map ByteBuffer.wrap map (_.getInt)

  def readLong(path: String, stat: Stat): Option[Long] = read(path, stat) map ByteBuffer.wrap map (_.getLong)

  def readLong(path: String): Option[Long] = read(path) map ByteBuffer.wrap map (_.getLong)

  def readString(path: String, stat: Stat): Option[String] = read(path, stat) map (new String(_, encoding))

  def readString(path: String): Option[String] = read(path) map (new String(_, encoding))

  def reconnect() {
    Try(zk.close())
    zk = new ZooKeeper(host, port, new MyWatcher(callback))
  }

  def remoteHost = s"$host:$port"

  /**
   * Updates the given path
   */
  def updateAtomic(path: String, data: Array[Byte], stat: Stat): Seq[OpResult] = {
    batch(
      Op.delete(path, stat.getVersion),
      Op.create(path, data, acl, mode))
  }

  def update(path: String, data: Array[Byte]): Iterable[String] = {
    delete(path)
    create(path -> data)
  }

  def updateLong(path: String, value: Long): Iterable[String] = {
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

