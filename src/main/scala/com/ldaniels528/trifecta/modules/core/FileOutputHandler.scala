package com.ldaniels528.trifecta.modules.core

import java.io._

import com.ldaniels528.trifecta.support.io.BinaryOutputHandler

import scala.concurrent.ExecutionContext

/**
 * File Output Handler
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class FileOutputHandler(out: OutputStream) extends BinaryOutputHandler {
  private val dos = new DataOutputStream(out)

  override def write(key: Array[Byte], message: Array[Byte])(implicit ec: ExecutionContext) = {
    // persist the key
    dos.writeInt(key.length)
    dos.write(key)

    // persist the message
    dos.writeInt(message.length)
    dos.write(message)
  }

  override def close() = dos.close()

}

/**
 * File Output Handler Singleton
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
object FileOutputHandler {

  def apply(path: String): FileOutputHandler = {
    new FileOutputHandler(new FileOutputStream(path))
  }

  def apply(file: File): FileOutputHandler = {
    new FileOutputHandler(new FileOutputStream(file))
  }

}
