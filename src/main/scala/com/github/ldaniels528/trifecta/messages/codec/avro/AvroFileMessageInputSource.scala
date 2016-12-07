package com.github.ldaniels528.trifecta.messages.codec.avro

import java.io.{DataInputStream, File, FileInputStream, InputStream}
import java.net.URL

import com.github.ldaniels528.trifecta.messages.{KeyAndMessage, MessageInputSource}

/**
 * Avro File Input Source
 * @author lawrence.daniels@gmail.com
 */
class AvroFileMessageInputSource(in: InputStream) extends MessageInputSource {
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
 * @author lawrence.daniels@gmail.com
 */
object AvroFileMessageInputSource {

  def apply(resourcePath: String): AvroFileMessageInputSource = {
    val in = Option(getClass.getResource(resourcePath)) map (_.openStream()) getOrElse new FileInputStream(resourcePath)
    new AvroFileMessageInputSource(in)
  }

  def apply(file: File): AvroFileMessageInputSource = {
    new AvroFileMessageInputSource(new FileInputStream(file))
  }

  def apply(url: URL): AvroFileMessageInputSource = {
    new AvroFileMessageInputSource(url.openStream())
  }

}