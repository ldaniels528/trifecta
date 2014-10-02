package com.ldaniels528.trifecta.modules.core

import java.io.{DataInputStream, File, FileInputStream, InputStream}
import java.net.URL

import com.ldaniels528.trifecta.support.io.{InputSource, KeyAndMessage}

/**
 * File Input Source
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class FileInputSource(in: InputStream) extends InputSource {
  private val dis = new DataInputStream(in)

  override def read: Option[KeyAndMessage] = {
    if (dis.available() == 0) None
    else {
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

      Option(KeyAndMessage(key, message))
    }
  }

  override def close() = dis.close()

}

/**
 * File Input Handler Singleton
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
object FileInputSource {

  def apply(resourcePath: String): FileInputSource = {
    val in = Option(getClass.getResource(resourcePath)) map (_.openStream()) getOrElse new FileInputStream(resourcePath)
    new FileInputSource(in)
  }

  def apply(file: File): FileInputSource = {
    new FileInputSource(new FileInputStream(file))
  }

  def apply(url: URL): FileInputSource = {
    new FileInputSource(url.openStream())
  }

}