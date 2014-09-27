package com.ldaniels528.trifecta.modules

import java.net.{URL, URLClassLoader}

import com.ldaniels528.trifecta.TxRuntimeContext
import com.ldaniels528.trifecta.command.{UnixLikeArgs, Command}
import com.ldaniels528.trifecta.modules.io.OutputWriter
import com.ldaniels528.trifecta.util.TxUtils._
import com.ldaniels528.trifecta.vscript.Variable

import scala.util.{Failure, Success, Try}

/**
 * Represents a dynamically loadable module
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
trait Module {

  /**
   * Returns the name of the module (e.g. "kafka")
   * @return the name of the module
   */
  def moduleName: String

  /**
   * Returns the commands that are bound to the module
   * @return the commands that are bound to the module
   */
  def getCommands(implicit rt: TxRuntimeContext): Seq[Command]

  /**
   * Returns an output writer for the given module
   * @param path the given output path
   * @return the option of an output writer
   */
  def getOutput(path: String): Option[OutputWriter]

  /**
   * Returns the variables that are bound to the module
   * @return the variables that are bound to the module
   */
  def getVariables: Seq[Variable]

  /**
   * Returns the the information that is to be displayed while the module is active
   * @return the the information that is to be displayed while the module is active
   */
  def prompt: String = s"$moduleName$$"

  /**
   * Called when the application is shutting down
   */
  def shutdown(): Unit

  protected def die[S](message: String): S = throw new IllegalArgumentException(message)

  protected def dieSyntax[S](unixArgs: UnixLikeArgs): S = {
    die(s"""Invalid arguments - use "syntax ${unixArgs.commandName.get}" to see usage""")
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

  protected def handleOutput(params: UnixLikeArgs, key: Array[Byte], message: Array[Byte])(implicit rt: TxRuntimeContext, ec: ExecutionContext) {
    params("-o") foreach { device =>
      device.split(':').toList match {
        case prefix :: path :: Nil =>
          rt.getOutput(prefix, path) getOrElse die("No such output device") use (_.write(key, message))
        case _ => die(s"Output device '$device' not recognized; usage: -o deviceType:devicePath")
      }
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

}