package com.github.ldaniels528.trifecta.io.zookeeper

import kafka.utils.ZKStringSerializer
import org.I0Itec.zkclient.ZkClient
import org.apache.zookeeper.CreateMode

import scala.collection.JavaConversions._
import scala.util.Try

/**
  * ZooKeeper Proxy (com.101tec implementation)
  * @author lawrence.daniels@gmail.com
  */
case class ZKProxy101tec(connectionString: String, timeOut: Int = 5000) extends ZKProxy {
  private val empty = new Array[Byte](0)
  var clientI0Itec: ZkClient = _

  connect()

  override def close(): Unit = {
    Try(clientI0Itec.close())
    clientI0Itec = null
  }

  override def connect(): Unit = {
    clientI0Itec = new ZkClient(connectionString, Integer.MAX_VALUE, timeOut, ZKStringSerializer)
  }

  override def create(tupleSeq: (String, Array[Byte])*): Iterable[String] = {
    tupleSeq.map { case (path, data) =>
      clientI0Itec.create(path, data, CreateMode.PERSISTENT)
    }
  }

  override def create(path: String, data: Array[Byte]): String = {
    clientI0Itec.create(path, data, CreateMode.PERSISTENT)
  }

  override def createDirectory(path: String): String = {
    clientI0Itec.create(path, empty, CreateMode.PERSISTENT)
  }

  override def ensurePath(path: String): List[String] = {
    List(clientI0Itec.create(path, empty, CreateMode.PERSISTENT))
  }

  override def ensureParents(path: String): List[String] = {
    List(clientI0Itec.create(path, empty, CreateMode.PERSISTENT))
  }

  override def delete(path: String): Boolean = clientI0Itec.delete(path)

  override def deleteRecursive(path: String): Boolean = clientI0Itec.delete(path)

  override def exists(path: String): Boolean = clientI0Itec.exists(path)

  override def getChildren(path: String, watch: Boolean): Seq[String] = {
    Option(clientI0Itec.getChildren(path)).toSeq.flatMap(_.toSeq)
  }

  override def getCreationTime(path: String): Option[Long] = Option(clientI0Itec.getCreationTime(path))

  override def getModificationTime(path: String): Option[Long] = Option(clientI0Itec.getCreationTime(path)) // TODO get modification time

  override def read(path: String): Option[Array[Byte]] = Option(clientI0Itec.readData[String](path, true)).map(_.getBytes())

  override def readString(path: String): Option[String] = Option(clientI0Itec.readData(path, true))

  override def update(path: String, data: Array[Byte]): Option[String] = Option(clientI0Itec.writeData(path, data)).map(_ => path)

}
