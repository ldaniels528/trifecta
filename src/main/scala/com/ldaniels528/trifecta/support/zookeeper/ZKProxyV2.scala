package com.ldaniels528.trifecta.support.zookeeper

import java.nio.ByteBuffer
import java.util

import org.I0Itec.zkclient.ZkClient
import org.apache.zookeeper.CreateMode
import org.apache.zookeeper.CreateMode._
import org.apache.zookeeper.ZooDefs.Ids
import org.apache.zookeeper.data.{ACL, Stat}
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._
import scala.language.implicitConversions
import scala.util.Try

/**
 * ZooKeeper Proxy (Version 2.0)
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class ZKProxyV2(host: String, port: Int) extends ZKProxy {
  private val logger = LoggerFactory.getLogger(getClass)
  private val NO_DATA = new Array[Byte](0)
  private var zk = new ZkClient(s"$host:$port")
  var acl: util.ArrayList[ACL] = Ids.OPEN_ACL_UNSAFE
  var mode: CreateMode = PERSISTENT
  var encoding: String = "UTF8"

  def client: ZkClient = zk

  override def close(): Unit = zk.close()

  override def create(path: String, data: Array[Byte]) = zk.create(path, data, CreateMode.PERSISTENT)

  override def create(tuples: (String, Array[Byte])*): Iterable[String] = {
    tuples.map { case (path, data) => create(path, data)}
  }

  override def createDirectory(path: String): String = {
    zk.create(path, NO_DATA, mode)
  }

  override def delete(path: String) = zk.delete(path)

  override def exists(path: String): Boolean = zk.exists(path)

  override def exists_?(path: String, watch: Boolean): Option[Stat] = ???

  override def getChildren(path: String, watch: Boolean): Seq[String] = zk.getChildren(path).toSeq

  override def getCreationTime(path: String): Option[Long] = Option(zk.getCreationTime(path))

  override def read(path: String): Option[Array[Byte]] = Option(zk.readData(path))

  override def readDouble(path: String): Option[Double] = read(path) map ByteBuffer.wrap map (_.getDouble)

  override def readInt(path: String): Option[Int] = read(path) map ByteBuffer.wrap map (_.getInt)

  override def readLong(path: String): Option[Long] = read(path) map ByteBuffer.wrap map (_.getLong)

  override def readString(path: String): Option[String] = read(path) map (new String(_, encoding))

  override def reconnect() {
    Try(zk.close())
    zk = new ZkClient(s"$host:$port")
  }

  override def update(path: String, data: Array[Byte]): Iterable[String] = ???

  override def ensurePath(path: String): List[String] = ???

  override def ensureParents(path: String): List[String] = ???

  override def updateLong(path: String, value: Long): Iterable[String] = ???

  override def getFamily(path: String): List[String] = ???

  override def remoteHost: String = s"$host:$port"


}
