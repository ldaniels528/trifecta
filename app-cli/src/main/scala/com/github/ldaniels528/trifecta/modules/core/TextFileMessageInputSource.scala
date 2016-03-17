package com.github.ldaniels528.trifecta.modules.core

import java.io._
import java.net.URL

import com.github.ldaniels528.trifecta.io.{ByteBufferUtils, KeyAndMessage, MessageInputSource}

/**
 * Text File Input Source
 * @author lawrence.daniels@gmail.com
 */
class TextFileMessageInputSource(in: InputStream) extends MessageInputSource {
  private val reader = new BufferedReader(new InputStreamReader(in))

  override def read: Option[KeyAndMessage] = {
    Option(reader.readLine()) map { line =>
      KeyAndMessage(ByteBufferUtils.longToBytes(System.currentTimeMillis()), line.getBytes("UTF-8"))
    }
  }

  override def close() = reader.close()

}

/**
 * Text File Input Source Singleton
 * @author lawrence.daniels@gmail.com
 */
object TextFileMessageInputSource {

  def apply(resourcePath: String): TextFileMessageInputSource = {
    val in = Option(getClass.getResource(resourcePath)) map (_.openStream()) getOrElse new FileInputStream(resourcePath)
    new TextFileMessageInputSource(in)
  }

  def apply(file: File): TextFileMessageInputSource = {
    new TextFileMessageInputSource(new FileInputStream(file))
  }

  def apply(url: URL): TextFileMessageInputSource = {
    new TextFileMessageInputSource(url.openStream())
  }

}