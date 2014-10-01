package com.ldaniels528.trifecta.modules.core

import java.io._

import com.ldaniels528.trifecta.support.io.{KeyAndMessage, OutputHandler}
import com.ldaniels528.trifecta.support.messaging.MessageDecoder

import scala.concurrent.ExecutionContext

/**
 * File Output Handler
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class FileOutputHandler(out: OutputStream) extends OutputHandler {
  private val dos = new DataOutputStream(out)

  override def write(data: KeyAndMessage, decoder: Option[MessageDecoder[_]])(implicit ec: ExecutionContext) = {
    // persist the key
    dos.writeInt(data.key.length)
    dos.write(data.key)

    // persist the message
    dos.writeInt(data.message.length)
    dos.write(data.message)
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
