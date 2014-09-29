package com.ldaniels528.trifecta.modules.zookeeper

import com.ldaniels528.trifecta.support.avro.AvroDecoder
import com.ldaniels528.trifecta.support.io.{BinaryOutputHandler, MessageOutputHandler}
import com.ldaniels528.trifecta.support.messaging.MessageDecoder
import com.ldaniels528.trifecta.support.zookeeper.ZKProxy

import scala.concurrent.ExecutionContext
import scala.util.{Try, Failure, Success}

/**
 * Zookeeper Output Writer
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class ZookeeperOutputHandler(zk: ZKProxy, rootPath: String) extends BinaryOutputHandler with MessageOutputHandler {

  /**
   * Returns the binary encoding
   * @return the binary encoding
   */
  val encoding: String = "UTF8"

  /**
   * Writes the given key-message pair to the underlying stream
   * @param key the given key
   * @param message the given message
   * @return the response value
   */
  override def write(key: Array[Byte], message: Array[Byte])(implicit ec: ExecutionContext) = {
    val path = s"$rootPath/${new String(key, encoding)}"
    zk.create(path, message)
  }

  /**
   * Writes the given key and decoded message to the underlying stream
   * @param key the given key
   * @param message the given message
   * @return the response value
   */
  override def write(decoder: Option[MessageDecoder[_]], key: Array[Byte], message: Array[Byte])(implicit ec: ExecutionContext) = {
    decoder match {
      case Some(av: AvroDecoder) =>
        av.decode(message) match {
          case Success(record) =>
            val path = s"$rootPath/${new String(key, encoding)}"
            zk.create(path, message)
          case Failure(e) =>
            throw new IllegalStateException(e.getMessage, e)
        }
      case Some(unhandled) =>
        throw new IllegalStateException(s"Unhandled decoder '$unhandled'")
      case None => write(rootPath.getBytes(encoding), message)
    }
  }

  override def close(): Unit = {
    Try(zk.close())
    ()
  }

}
