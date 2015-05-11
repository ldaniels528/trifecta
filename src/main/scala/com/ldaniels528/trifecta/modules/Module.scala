package com.ldaniels528.trifecta.modules

import java.net.{URL, URLClassLoader}
import java.nio.ByteBuffer

import com.ldaniels528.trifecta.TxRuntimeContext
import com.ldaniels528.trifecta.command.parser.CommandParser._
import com.ldaniels528.trifecta.command.{Command, UnixLikeArgs}
import com.ldaniels528.trifecta.io._
import com.ldaniels528.trifecta.modules.Module.formatTypes
import com.ldaniels528.commons.helpers.ProcessHelper._
import net.liftweb.json._

/**
 * Represents a dynamically loadable module
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
trait Module {

  /**
   * Returns the commands that are bound to the module
   * @return the commands that are bound to the module
   */
  def getCommands(implicit rt: TxRuntimeContext): Seq[Command]

  /**
   * Attempts to retrieve an input source for the given URL
   * @param url the given input URL
   * @return the option of an input source
   */
  def getInputSource(url: String): Option[InputSource]

  /**
   * Attempts to retrieve an output source for the given URL
   * @param url the given output URL
   * @return the option of an output source
   */
  def getOutputSource(url: String): Option[OutputSource]

  /**
   * Returns the name of the module (e.g. "kafka")
   * @return the name of the module
   */
  def moduleName: String

  /**
   * Returns the label of the module (e.g. "kafka")
   * @return the label of the module
   */
  def moduleLabel: String

  /**
   * Returns the the information that is to be displayed while the module is active
   * @return the the information that is to be displayed while the module is active
   */
  def prompt: String = s"$moduleName$$"

  /**
   * Called when the application is shutting down
   */
  def shutdown(): Unit

  /**
   * Returns the name of the prefix (e.g. Seq("file"))
   * @return the name of the prefix
   */
  def supportedPrefixes: Seq[String]

  protected def die[S](message: String): S = throw new IllegalArgumentException(message)

  protected def dieInvalidOutputURL[S](url: String, example: String): S = die(s"Invalid output URL '$url' - Example usage: $example")

  protected def dieNoInputHandler[S](device: InputSource): S = die(s"Unhandled input source $device")

  protected def dieNoOutputHandler[S](device: OutputSource): S = die(s"Unhandled output source $device")

  protected def dieSyntax[S](unixArgs: UnixLikeArgs): S = {
    die( s"""Invalid arguments - use "syntax ${unixArgs.commandName.get}" to see usage""")
  }

  protected def decodeValue(bytes: Array[Byte], valueType: String): Any = {
    valueType match {
      case "bytes" => bytes
      case "char" => ByteBuffer.wrap(bytes).getChar
      case "double" => ByteBuffer.wrap(bytes).getDouble
      case "float" => ByteBuffer.wrap(bytes).getFloat
      case "int" | "integer" => ByteBuffer.wrap(bytes).getInt
      case "json" => formatJson(new String(bytes))
      case "long" => ByteBuffer.wrap(bytes).getLong
      case "short" => ByteBuffer.wrap(bytes).getShort
      case "string" | "text" => new String(bytes)
      case _ => throw new IllegalArgumentException(s"Invalid type format '$valueType'. Acceptable values are: ${formatTypes mkString ", "}")
    }
  }

  protected def encodeValue(value: String, valueType: String): Array[Byte] = {
    import java.nio.ByteBuffer.allocate

    valueType match {
      case "bytes" => parseDottedHex(value)
      case "char" => allocate(2).putChar(value.head).array()
      case "double" => allocate(8).putDouble(value.toDouble).array()
      case "float" => allocate(4).putFloat(value.toFloat).array()
      case "int" | "integer" => allocate(4).putInt(value.toInt).array()
      case "long" => allocate(8).putLong(value.toLong).array()
      case "short" => allocate(2).putShort(value.toShort).array()
      case "string" | "text" => value.getBytes
      case _ => throw new IllegalArgumentException(s"Invalid type '$valueType'")
    }
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

  private def formatJson(value: String): String = pretty(render(parse(value)))

  protected def getInputSource(params: UnixLikeArgs)(implicit rt: TxRuntimeContext): Option[InputSource] = {
    params("-i") flatMap rt.getInputHandler
  }

  protected def getOutputSource(params: UnixLikeArgs)(implicit rt: TxRuntimeContext): Option[OutputSource] = {
    params("-o") flatMap rt.getOutputHandler
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

/**
 * Module Companion Object
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
object Module {
  val formatTypes = Seq("bytes", "char", "double", "float", "int", "json", "long", "short", "string")

  /**
   * A simple name-value pair
   * @param name the name of the property
   * @param value the value of the property
   */
  case class NameValuePair(name: String, value: Any)

}