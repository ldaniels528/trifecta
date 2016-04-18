package com.github.ldaniels528.trifecta.io.json

import java.io._
import java.net.URL

import com.github.ldaniels528.trifecta.io.{ByteBufferUtils, MessageInputSource, KeyAndMessage}

/**
 * JSON File Input Source
 * @author lawrence.daniels@gmail.com
 */
class JSONFileMessageInputSource(in: InputStream) extends MessageInputSource {
  private val reader = new BufferedReader(new InputStreamReader(in))

  override def read: Option[KeyAndMessage] = {
    Option(reader.readLine()) map { js =>
      KeyAndMessage(ByteBufferUtils.longToBytes(System.currentTimeMillis()), js.getBytes("UTF-8"))
    }
  }

  override def close() = reader.close()

}

/**
 * File Input Handler Singleton
 * @author lawrence.daniels@gmail.com
 */
object JSONFileMessageInputSource {

  def apply(resourcePath: String): JSONFileMessageInputSource = {
    val in = Option(getClass.getResource(resourcePath)) map (_.openStream()) getOrElse new FileInputStream(resourcePath)
    new JSONFileMessageInputSource(in)
  }

  def apply(file: File): JSONFileMessageInputSource = {
    new JSONFileMessageInputSource(new FileInputStream(file))
  }

  def apply(url: URL): JSONFileMessageInputSource = {
    new JSONFileMessageInputSource(url.openStream())
  }

}