package com.ldaniels528.verify.subsystems.zookeeper

import ZKProxy._
import ZKProxy.Implicits._
import com.ldaniels528.verify.io.EndPoint

/**
 * Verify ZooKeeper Proxy
 * @author lawrence.daniels@gmail.com
 */
class ZKProxyNew(host: String, port: Int) {
  import org.slf4j.LoggerFactory
  import org.I0Itec.zkclient.ZkClient

  private val logger = LoggerFactory.getLogger(getClass())
  private var zk = new ZkClient(s"$host:$port")

  def client: ZkClient = zk

  def delete(path: String) = zk.delete(path)

  def exists(path: String) = zk.exists(path)

  def getChildren(path: String) = zk.getChildren(path)

  def read(path: String): Array[Byte] = zk.readData(path)

  def readString(path: String): String = zk.readData(path)

}

/**
 * Zookeeper Proxy Singleton
 * @author lawrence.daniels@gmail.com
 */
object ZKProxyNew {
  private val NO_DATA = new Array[Byte](0)

  def apply(ep: EndPoint) = new ZKProxyNew(ep.host, ep.port)

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