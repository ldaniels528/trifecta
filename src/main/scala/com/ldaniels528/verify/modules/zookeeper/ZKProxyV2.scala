package com.ldaniels528.verify.modules.zookeeper

import java.nio.ByteBuffer

import org.I0Itec.zkclient.ZkClient
import org.slf4j.LoggerFactory

import scala.language.implicitConversions
import scala.util.Try

/**
 * ZooKeeper Proxy (Version 2.0)
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class ZKProxyV2(host: String, port: Int) {
  private val logger = LoggerFactory.getLogger(getClass)
  private var zk = new ZkClient(s"$host:$port")
  var encoding: String = "UTF8"

  def client: ZkClient = zk

  def delete(path: String) = zk.delete(path)

  def exists(path: String) = zk.exists(path)

  def getChildren(path: String) = zk.getChildren(path)

  def read(path: String): Option[Array[Byte]] = Option(zk.readData(path))

  def readDouble(path: String): Option[Double] = read(path) map ByteBuffer.wrap map (_.getDouble)

  def readInt(path: String): Option[Int] = read(path) map ByteBuffer.wrap map (_.getInt)

  def readLong(path: String): Option[Long] = read(path) map ByteBuffer.wrap map (_.getLong)

  def readString(path: String): Option[String] = read(path) map (new String(_, encoding))

  def reconnect() {
    Try(zk.close())
    zk = new ZkClient(s"$host:$port")
  }

}
