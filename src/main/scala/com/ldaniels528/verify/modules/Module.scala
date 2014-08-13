package com.ldaniels528.verify.modules

import java.nio.ByteBuffer._

import com.ldaniels528.verify.modules.Module._
import org.slf4j.LoggerFactory

/**
 * Represents a dynamically loadable module
 * @author lawrence.daniels@gmail.com
 */
trait Module {
  // logger instance
  protected val logger = LoggerFactory.getLogger(getClass)

  /**
   * Returns the name of the module (e.g. "kafka")
   * @return the name of the module
   */
  def name: String

  /**
   * Returns the commands that are bound to the module
   * @return the commands that are bound to the module
   */
  def getCommands: Seq[Command]

  /**
   * Returns the the information that is to be displayed while the module is active
   * @return the the information that is to be displayed while the module is active
   */
  def prompt: String = s"$name$$"

  /**
   * Called when the application is shutting down
   */
  def shutdown(): Unit

  protected def asChars(bytes: Array[Byte]): String = {
    String.valueOf(bytes map (b => if (b >= 32 && b <= 126) b.toChar else '.'))
  }

  /**
   * Expands the UNIX path into a JVM-safe value
   * @param path the UNIX path (e.g. "~/ldaniels")
   * @return a JVM-safe value (e.g. "/home/ldaniels")
   */
  protected def expandPath(path: String): String = {
    path.replaceFirst("[~/]", scala.util.Properties.userHome)
  }

  protected def extract(args: Seq[String], index: Int): Option[String] = {
    if (args.length > index) Some(args(index)) else None
  }

  /**
   * Attempts to extract the value from the sequence at the given index
   * @param values the given sequence of values
   * @param index the given index
   * @return the option of the value
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
  case class Command(module:Module, name: String, fx: Seq[String] => Any, params: (Seq[String], Seq[String]) = (Seq.empty, Seq.empty), help: String = "") {

    def prototype = {
      val required = (params._1 map (s => s"<$s>")).mkString(" ")
      val optional = (params._2 map (s => s"<$s>")).mkString(" ")
      s"$name $required ${if (optional.nonEmpty) s"[$optional]" else ""}"
    }
  }

}