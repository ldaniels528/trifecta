package com.github.ldaniels528.trifecta.io.zookeeper

import scala.language.implicitConversions

/**
 * ZooKeeper Proxy
 * @author lawrence.daniels@gmail.com
 */
trait ZKProxy {

  def close(): Unit

  def connect(): Unit

  def connectionString: String

  def create(tupleSeq: (String, Array[Byte])*): Iterable[String]

  def create(path: String, data: Array[Byte]): String

  def createDirectory(path: String): String

  def ensurePath(path: String): List[String]

  def ensureParents(path: String): List[String]

  def delete(path: String): Boolean

  def deleteRecursive(path: String): Boolean

  def exists(path: String): Boolean

  def getChildren(path: String, watch: Boolean = false): Seq[String]

  def getCreationTime(path: String): Option[Long]

  def getFamily(path: String): List[String]

  def getModificationTime(path: String): Option[Long]

  def read(path: String): Option[Array[Byte]]

  def readString(path: String): Option[String]

  def update(path: String, data: Array[Byte]): Option[String]

}

/**
 * ZooKeeper Proxy Companion Object
 * @author lawrence.daniels@gmail.com
 */
object ZKProxy {

  def apply(connectionString: String) = new ZkProxyCurator(connectionString)

  /**
   * All implicit definitions are declared here
   * @author lawrence.daniels@gmail.com
   */
  object Implicits {

    import java.nio.ByteBuffer

    implicit def byteBuffer2ByteArray(buf: ByteBuffer): Array[Byte] = {
      val bytes = new Array[Byte](buf.limit())
      buf.rewind()
      buf.get(bytes)
      bytes
    }

    implicit class ZKPathSplitter(val path: String) extends AnyVal {

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
