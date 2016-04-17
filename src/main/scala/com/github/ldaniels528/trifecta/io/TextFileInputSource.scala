package com.github.ldaniels528.trifecta.io

import java.io._
import java.net.URL

/**
 * Text File Input Source
 * @author lawrence.daniels@gmail.com
 */
class TextFileInputSource(in: InputStream) extends InputSource {
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
object TextFileInputSource {

  def apply(resourcePath: String): TextFileInputSource = {
    val in = Option(getClass.getResource(resourcePath)) map (_.openStream()) getOrElse new FileInputStream(resourcePath)
    new TextFileInputSource(in)
  }

  def apply(file: File): TextFileInputSource = {
    new TextFileInputSource(new FileInputStream(file))
  }

  def apply(url: URL): TextFileInputSource = {
    new TextFileInputSource(url.openStream())
  }

}