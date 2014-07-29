package com.ldaniels528.verify.subsystems.zookeeper

import ZKProxy._
import ZKProxy.Implicits._
import org.apache.zookeeper.Watcher
import com.ldaniels528.verify.io.EndPoint

/**
 * Verify ZooKeeper Proxy
 * @author lawrence.daniels@gmail.com
 */
class ZKProxy(host: String, port: Int, watcher: Watcher) {
  import java.util.ArrayList
  import java.nio.ByteBuffer
  import scala.util.{ Try, Success, Failure }
  import scala.collection.JavaConversions._
  import scala.language.implicitConversions
  import org.slf4j.LoggerFactory
  import org.apache.zookeeper._
  import org.apache.zookeeper.ZooDefs.Ids
  import org.apache.zookeeper.ZooKeeper
  import org.apache.zookeeper.data.{ ACL, Stat }
  import AsyncCallback.{ StatCallback, StringCallback }
  import CreateMode.PERSISTENT

  private val logger = LoggerFactory.getLogger(getClass())
  private var zk = new ZooKeeper(host, port, watcher)
  var acl: ArrayList[ACL] = Ids.OPEN_ACL_UNSAFE
  var mode: CreateMode = PERSISTENT
  var encoding: String = "UTF8"

  def batch(ops: Op*): Seq[OpResult] = zk.multi(ops)

  def client: ZooKeeper = zk

  def reconnect() {
    Try(zk.close())
    zk = new ZooKeeper(host, port, watcher)
  }

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
    logger.info(s"create parent paths: ${tuples map (_._1) mkString (",")}")
    tuples foreach {
      case (node, data) =>
        if (exists(node).isEmpty) zk.create(node, data, acl, mode)
    }
    this
  }

  def ensureParents(path: String): ZKProxy = {
    val tuples = path.splitNodes.init map (p => (p, NO_DATA))
    logger.info(s"create parent paths: ${tuples map (_._1) mkString (",")}")
    tuples foreach {
      case (node, data) =>
        if (exists(node).isEmpty) zk.create(node, data, acl, mode)
    }
    this
  }

  def delete(path: String, stat: Stat) {
    zk.delete(path, stat.getVersion())
  }

  def exists(path: String, watch: Boolean = false): Option[Stat] = {
    Option(zk.exists(path, watch))
  }

  def getChildren(path: String, watch: Boolean = false): Seq[String] = {
    zk.getChildren(path, watch)
  }

  def getSessionId() = zk.getSessionId()

  def getState() = zk.getState()

  def read(path: String, stat: Stat): Array[Byte] = {
    zk.getData(path, false, stat)
  }

  def read(path: String): Option[Array[Byte]] = {
    exists(path) map { stat =>
      zk.getData(path, false, stat)
    }
  }

  def readDouble(path: String, stat: Stat): Double = {
    ByteBuffer.wrap(zk.getData(path, false, stat)).getDouble()
  }

  def readDouble(path: String): Option[Double] = {
    exists(path) map { stat =>
      ByteBuffer.wrap(zk.getData(path, false, stat)).getDouble()
    }
  }

  def readInt(path: String, stat: Stat): Int = {
    ByteBuffer.wrap(zk.getData(path, false, stat)).getInt()
  }

  def readInt(path: String): Option[Int] = {
    exists(path) map { stat =>
      ByteBuffer.wrap(zk.getData(path, false, stat)).getInt()
    }
  }

  def readLong(path: String, stat: Stat): Long = {
    ByteBuffer.wrap(zk.getData(path, false, stat)).getLong()
  }

  def readLong(path: String): Option[Long] = {
    exists(path) map { stat =>
      ByteBuffer.wrap(zk.getData(path, false, stat)).getLong()
    }
  }

  def readString(path: String, stat: Stat): String = {
    new String(zk.getData(path, false, stat), encoding)
  }

  def readString(path: String): Option[String] = {
    exists(path) map { stat =>
      new String(zk.getData(path, false, stat), encoding)
    }
  }

  /**
   * Updates the given path
   *
   * def update(path: String, data: Array[Byte], stat: Stat): Seq[OpResult] = {
   * batch(
   * Op.delete(path, stat.getVersion()),
   * Op.create(path, data, acl, mode))
   * }
   */

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
 * @author lawrence.daniels@gmail.com
 */
object ZKProxy {
  private val NO_DATA = new Array[Byte](0)

  def apply(ep: EndPoint, watcher: Watcher) = new ZKProxy(ep.host, ep.port, watcher)

  /**
   * All implicit definitions are declared here
   */
  object Implicits {
    import java.nio.ByteBuffer
    import scala.language.implicitConversions

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
