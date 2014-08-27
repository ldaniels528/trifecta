package com.ldaniels528.verify.modules

import com.ldaniels528.verify.util.VxUtils._

import scala.util.{Failure, Try, Success}

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

  protected def parseInt(label:String, value: String): Int = {
    Try(value.toInt) match {
      case Success(v) => v
      case Failure(e) =>
        throw new IllegalArgumentException(s"$label: Expected an integer value, found '$value'")
    }
  }

  protected def parseLong(label:String, value: String): Long = {
    Try(value.toLong) match {
      case Success(v) => v
      case Failure(e) =>
        throw new IllegalArgumentException(s"$label: Expected an integer value, found '$value'")
    }
  }

  /**
   * Executes a Java application via its "main" method
   * @param className the name of the class to invoke
   * @param args the arguments to pass to the application
   */
  protected def runJava(jarPath: String, className: String, args: String*): Iterator[String] = {
    import scala.io.Source

    val classUrls = Array(new URL(s"file://./$jarPath"))
    val classLoader = new URLClassLoader(classUrls)

    // reset the buffer
    val (out, _, _) = sandbox(initialSize = 1024) {
      // execute the command
      val jarClass = classLoader.loadClass(className)
      val mainMethod = jarClass.getMethod("main", classOf[Array[String]])
      mainMethod.invoke(null, args.toArray)
    }

    // return the iteration of lines
    Source.fromBytes(out).getLines()
  }

}