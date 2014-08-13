package com.ldaniels528.verify.subsystems

import java.nio.ByteBuffer._

import com.ldaniels528.verify.subsystems.Module._
import org.slf4j.LoggerFactory

/**
 * Represents a dynamically loaded module
 * @author lawrence.daniels@gmail.com
 */
trait Module {
  // logger instance
  protected val logger = LoggerFactory.getLogger(getClass)

  def getCommands: Seq[Command]

  def shutdown(): Unit

  protected def asChars(bytes: Array[Byte]): String = {
    String.valueOf(bytes map (b => if (b >= 32 && b <= 126) b.toChar else '.'))
  }

  protected def expandPath(path: String): String = {
    path.replaceFirst("[~/]", scala.util.Properties.userHome)
  }

  protected def extract(args: Seq[String], index: Int): Option[String] = {
    if (args.length > index) Some(args(index)) else None
  }

  /**
   * Converts the given long value into a byte array
   * @param value the given long value
   * @return a byte array
   */
  protected def toBytes(value: Long): Array[Byte] = allocate(8).putLong(value).array()

}

/**
 * Module Companion Object
 * @author lawrence.daniels@gmail.com
 */
object Module {

  /**
   * Represents an Verify Shell command
   * @author lawrence.daniels@gmail.com
   */
  case class Command(name: String, fx: Seq[String] => Any, params: (Seq[String], Seq[String]) = (Seq.empty, Seq.empty), help: String = "") {

    def prototype = {
      val required = (params._1 map (s => s"<$s>")).mkString(" ")
      val optional = (params._2 map (s => s"<$s>")).mkString(" ")
      s"$name $required ${if (optional.nonEmpty) s"[$optional]" else ""}"
    }
  }

}