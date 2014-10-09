package com.ldaniels528.trifecta.modules

import java.net.{URL, URLClassLoader}

import com.ldaniels528.trifecta.command.{Command, UnixLikeArgs}
import com.ldaniels528.trifecta.support.avro.AvroDecoder
import com.ldaniels528.trifecta.support.io._
import com.ldaniels528.trifecta.util.TxUtils._
import com.ldaniels528.trifecta.vscript.Variable
import com.ldaniels528.trifecta.{TxConfig, TxRuntimeContext}

import scala.util.{Failure, Success, Try}

/**
 * Represents a dynamically loadable module
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
trait Module extends AvroReading {

  /**
   * Returns the commands that are bound to the module
   * @return the commands that are bound to the module
   */
  def getCommands(implicit rt: TxRuntimeContext): Seq[Command]

  /**
   * Attempts to retrieve an input handler for the given URL
   * @param url the given input URL
   * @return the option of an input handler
   */
  def getInputHandler(url: String): Option[InputSource]

  /**
   * Attempts to retrieve an output handler for the given URL
   * @param url the given output URL
   * @return the option of an output handler
   */
  def getOutputHandler(url: String): Option[OutputSource]

  /**
   * Returns the variables that are bound to the module
   * @return the variables that are bound to the module
   */
  def getVariables: Seq[Variable]

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

  /**
   * Expands the UNIX path into a JVM-safe value
   * @param path the UNIX path (e.g. "~/ldaniels")
   * @return a JVM-safe value (e.g. "/home/ldaniels")
   */
  protected def expandPath(path: String): String = {
    path.replaceFirst("[~]", scala.util.Properties.userHome)
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

  protected def getAvroDecoder(params: UnixLikeArgs)(implicit config: TxConfig): Option[AvroDecoder] = {
    params("-a") map lookupAvroDecoder
  }

  protected def getInputSource(params: UnixLikeArgs)(implicit rt: TxRuntimeContext): Option[InputSource] = {
    params("-i") flatMap rt.getInputHandler
  }

  protected def getOutputSource(params: UnixLikeArgs)(implicit rt: TxRuntimeContext): Option[OutputSource] = {
    params("-o") flatMap rt.getOutputHandler
  }

  protected def parseDelta(label:String, value: String): Int = {
    value.head match {
      case '+' => parseInt(label, value.tail)
      case _ => parseInt(label, value)
    }
  }

  protected def parseDouble(label: String, value: String): Double = {
    Try(value.toDouble) match {
      case Success(v) => v
      case Failure(e) =>
        throw new IllegalArgumentException(s"$label: Expected an decimal value, found '$value'")
    }
  }

  protected def parseInt(label: String, value: String): Int = {
    Try(value.toInt) match {
      case Success(v) => v
      case Failure(e) =>
        throw new IllegalArgumentException(s"$label: Expected an integer value, found '$value'")
    }
  }

  protected def parseLong(label: String, value: String): Long = {
    Try(value.toLong) match {
      case Success(v) => v
      case Failure(e) =>
        throw new IllegalArgumentException(s"$label: Expected an long integer value, found '$value'")
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

/**
 * Module Companion Object
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
object Module {

  /**
   * A simple name-value pair
   * @param name the name of the property
   * @param value the value of the property
   */
  case class NameValuePair(name: String, value: Any)

  /**
   * Represents the results of an I/O operation
   * @param read the number of records read
   * @param written the number of records written
   * @param failures the number of failed records
   * @param recordsPerSecond the transfer rate (in records/second)
   * @param runTimeSecs the complete process run time (in seconds)
   */
  case class IOCount(read: Long, written: Long, failures: Long, recordsPerSecond: Double, runTimeSecs: Double)

}