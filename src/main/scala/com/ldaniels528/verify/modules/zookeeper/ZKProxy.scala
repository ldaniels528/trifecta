package com.ldaniels528.verify.modules.zookeeper

import java.nio.ByteBuffer
import java.util

import com.ldaniels528.verify.io.EndPoint
import com.ldaniels528.verify.modules.zookeeper.ZKProxy.Implicits._
import com.ldaniels528.verify.modules.zookeeper.ZKProxy._
import org.apache.zookeeper.AsyncCallback.StringCallback
import org.apache.zookeeper.CreateMode.PERSISTENT
import org.apache.zookeeper.ZooDefs.Ids
import org.apache.zookeeper.data.{ACL, Stat}
import org.apache.zookeeper.{WatchedEvent, Watcher, _}
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._
import scala.language.implicitConversions
import scala.util.Try

/**
 * ZooKeeper Proxy
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class ZKProxy(host: String, port: Int, callback: Option[ZkProxyCallBack] = None) {
  private val logger = LoggerFactory.getLogger(getClass)
  var acl: util.ArrayList[ACL] = Ids.OPEN_ACL_UNSAFE
  var mode: CreateMode = PERSISTENT
  var encoding: String = "UTF8"

  logger.info(s"Connecting to ZooKeeper at '$host:$port'...")
  private var zk = new ZooKeeper(host, port, new MyWatcher(callback))

  //def batch(ops: Op*): Seq[OpResult] = zk.multi(ops)

  def client: ZooKeeper = zk

  def create(tuples: (String, Array[Byte])*): Iterable[String] = {
    tuples map {
      case (node, data) =>
        zk.create(node, data, acl, mode)
    }
  }

  def createAsync(path: String, data: Array[Byte], ctx: Any, callback: StringCallback) {
    zk.create(path, data, acl, mode, callback, ctx)
  }

  def ensurePath(path: String): ZKProxy = {
    val tuples = path.splitNodes map (p => (p, NO_DATA))
    logger.info(s"create parent paths: ${tuples map (_._1) mkString ","}")
    tuples foreach {
      case (node, data) =>
        if (exists(node).isEmpty) zk.create(node, data, acl, mode)
    }
    this
  }

  def ensureParents(path: String): ZKProxy = {
    val tuples = path.splitNodes.init map (p => (p, NO_DATA))
    logger.info(s"create parent paths: ${tuples map (_._1) mkString ","}")
    tuples foreach {
      case (node, data) =>
        if (exists(node).isEmpty) zk.create(node, data, acl, mode)
    }
    this
  }

  def delete(path: String, stat: Stat, recursive: Boolean = false) {
    if (recursive) {
      getChildren(path) foreach { subPath =>
        exists(subPath) foreach (delete(subPath, _, recursive))
      }
    }
    zk.delete(path, stat.getVersion)
  }

  def exists(path: String, watch: Boolean = false): Option[Stat] = {
    Option(zk.exists(path, watch))
  }

  def getChildren(path: String, watch: Boolean = false): Seq[String] = {
    zk.getChildren(path, watch)
  }

  def getSessionId = zk.getSessionId

  def getState = zk.getState

  def read(path: String, stat: Stat): Option[Array[Byte]] = {
    Option(zk.getData(path, false, stat))
  }

  def read(path: String): Option[Array[Byte]] = {
    exists(path) flatMap { stat =>
      Option(zk.getData(path, false, stat))
    }
  }

  def readDouble(path: String, stat: Stat): Option[Double] = {
    read(path, stat) map ByteBuffer.wrap map (_.getDouble)
  }

  def readDouble(path: String): Option[Double] = {
    read(path) map ByteBuffer.wrap map (_.getDouble)
  }

  def readInt(path: String, stat: Stat): Option[Int] = {
    read(path, stat) map ByteBuffer.wrap map (_.getInt)
  }

  def readInt(path: String): Option[Int] = {
    read(path) map ByteBuffer.wrap map (_.getInt)
  }

  def readLong(path: String, stat: Stat): Option[Long] = {
    read(path, stat) map ByteBuffer.wrap map (_.getLong)
  }

  def readLong(path: String): Option[Long] = {
    read(path) map ByteBuffer.wrap map (_.getLong)
  }

  def readString(path: String, stat: Stat): Option[String] = {
    read(path, stat) map (new String(_, encoding))
  }

  def readString(path: String): Option[String] = {
    read(path) map (new String(_, encoding))
  }

  def reconnect() {
    Try(zk.close())
    zk = new ZooKeeper(host, port, new MyWatcher(callback))
  }

  /**
   * Updates the given path
   *//*
  def updateAtomic(path: String, data: Array[Byte], stat: Stat): Seq[OpResult] = {
    batch(
      Op.delete(path, stat.getVersion),
      Op.create(path, data, acl, mode))
  }*/

  def update(path: String, data: Array[Byte], stat: Stat) = {
    exists(path) map { stat =>
      delete(path, stat)
      create(path -> data)
    }
  }

  def updateLong(path: String, value: Long, stat: Stat) = {
    // write the value to a byte array
    val data = new Array[Byte](8)
    ByteBuffer.wrap(data).putLong(value)

    // perform the update
    update(path, data, stat)
  }

  def close() {
    zk.close()
  }

}

/**
 * Zookeeper Proxy Singleton
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
object ZKProxy {
  private val NO_DATA = new Array[Byte](0)

  def apply(ep: EndPoint, callback: Option[ZkProxyCallBack] = None) = new ZKProxy(ep.host, ep.port, callback)

  /**
   * My ZooKeeper Watcher Callback
   * @param callback the [[ZkProxyCallBack]]
   */
  class MyWatcher(callback: Option[ZkProxyCallBack]) extends Watcher {
    override def process(event: WatchedEvent) = callback.foreach(_.process(event))
  }

  /**
   * All implicit definitions are declared here
   */
  object Implicits {

    import java.nio.ByteBuffer

    implicit def byteBuffer2ByteArray(buf: ByteBuffer): Array[Byte] = {
      val bytes = new Array[Byte](buf.limit())
      buf.rewind()
      buf.get(bytes)
      bytes
    }

    implicit class ZKPathSplitter(path: String) {

      def splitNodes: List[String] = {
        val pcs = path.split("[/]").tail
        val list = pcs.foldLeft[List[String]](Nil) { (list, cur) =>
          val path = if (list.nonEmpty) s"${list.head}/$cur" else cur
          path :: list
        }
        list.reverse map (s => "/" + s)
      }

    }

  }

}
