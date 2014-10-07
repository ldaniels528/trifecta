package com.ldaniels528.trifecta.modules.core

import java.io._
import java.net.URL

import com.ldaniels528.trifecta.support.io.{InputSource, KeyAndMessage}
import com.ldaniels528.trifecta.util.ByteBufferUtils

/**
 * JSON File Input Source
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class JSONFileInputSource(in: InputStream) extends InputSource {
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
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
object JSONFileInputSource {

  def apply(resourcePath: String): JSONFileInputSource = {
    val in = Option(getClass.getResource(resourcePath)) map (_.openStream()) getOrElse new FileInputStream(resourcePath)
    new JSONFileInputSource(in)
  }

  def apply(file: File): JSONFileInputSource = {
    new JSONFileInputSource(new FileInputStream(file))
  }

  def apply(url: URL): JSONFileInputSource = {
    new JSONFileInputSource(url.openStream())
  }

}