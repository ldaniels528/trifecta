package com.ldaniels528.verify.modules

/**
 * Represents a dynamically loadable module
 * @author lawrence.daniels@gmail.com
 */
trait Module {

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

  /**
   * Returns the ASCII array as a character string
   * @param bytes the byte array
   * @return a character string representing the given byte array
   */
  protected def asChars(bytes: Array[Byte]): String = {
    String.valueOf(bytes map (b => if (b >= 32 && b <= 126) b.toChar else '.'))
  }

  /**
   * Returns the byte array as a hex string
   * @param bytes the byte array
   * @return a hex string representing the given byte array
   */
  protected def asHexString(bytes: Array[Byte]): String = {
    bytes map ("%02x".format(_)) mkString "."
  }

  /**
   * Expands the UNIX path into a JVM-safe value
   * @param path the UNIX path (e.g. "~/ldaniels")
   * @return a JVM-safe value (e.g. "/home/ldaniels")
   */
  protected def expandPath(path: String): String = {
    path.replaceFirst("[~/]", scala.util.Properties.userHome)
  }

  /**
   * Attempts to extract the value from the sequence at the given index
   * @param values the given sequence of values
   * @param index the given index
   * @return the option of the value
   */
  protected def extract[T](values: Seq[T], index: Int): Option[T] = {
    if (values.length > index) Some(values(index)) else None
  }

}