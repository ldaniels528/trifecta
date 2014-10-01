package com.ldaniels528.trifecta.modules.core

import java.io.{DataInputStream, File, FileInputStream, InputStream}
import java.net.URL

import com.ldaniels528.trifecta.support.io.{BinaryInputHandler, KeyedMessage}

import scala.concurrent.ExecutionContext

/**
 * File Input Handler
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class FileInputHandler(in: InputStream) extends BinaryInputHandler {
  private val dis = new DataInputStream(in)

  override def read: KeyedMessage = {
    // retrieve the key
    val key: Array[Byte] = {
      val key = new Array[Byte](dis.readInt())
      dis.read(key)
      key
    }

    // retrieve the message
    val message: Array[Byte] = {
      val message = new Array[Byte](dis.readInt())
      dis.read(message)
      message
    }

    KeyedMessage(key, message)
  }

  override def close() = dis.close()

}

/**
 * File Input Handler Singleton
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
object FileInputHandler {

  def apply(resourcePath: String): FileInputHandler = {
    val in = Option(getClass.getResource(resourcePath)) map (_.openStream()) getOrElse new FileInputStream(resourcePath)
    new FileInputHandler(in)
  }

  def apply(file: File): FileInputHandler = {
    new FileInputHandler(new FileInputStream(file))
  }

  def apply(url: URL): FileInputHandler = {
    new FileInputHandler(url.openStream())
  }

}