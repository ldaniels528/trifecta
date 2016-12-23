package com.github.ldaniels528.trifecta.modules

import java.io.File
import java.net.{URL, URLClassLoader}
import java.nio.ByteBuffer

import com.github.ldaniels528.commons.helpers.ProcessHelper._
import com.github.ldaniels528.trifecta.{TxConfig, TxRuntimeContext}
import com.github.ldaniels528.trifecta.command.{Command, UnixLikeArgs}
import com.github.ldaniels528.trifecta.messages.codec.json.JsonHelper
import com.github.ldaniels528.trifecta.messages.{MessageInputSource, MessageOutputSource, MessageReader, MessageWriter}
import com.github.ldaniels528.trifecta.modules.Module.formatTypes
import com.github.ldaniels528.trifecta.util.ParsingHelper._
import org.slf4j.LoggerFactory

import scala.io.Source
import scala.util.{Failure, Success, Try}

/**
  * Represents a dynamically loadable module
  * @author lawrence.daniels@gmail.com
  */
trait Module extends ModuleCommandAgent with MessageReader with MessageWriter {

  /**
    * Deciphers the given module-specific value into an object that can
    * be represented at the console
    * @param value the given [[AnyRef value]]
    * @return the option of a deciphered value
    */
  def decipher(value: Any): Option[AnyRef] = None

  /**
    * Returns the commands that are bound to the module
    * @return the commands that are bound to the module
    */
  def getCommands(implicit rt: TxRuntimeContext): Seq[Command]

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

  protected def decodeValue(bytes: Array[Byte], valueType: String): Any = {
    valueType match {
      case "bytes" => bytes
      case "char" => ByteBuffer.wrap(bytes).getChar
      case "double" => ByteBuffer.wrap(bytes).getDouble
      case "float" => ByteBuffer.wrap(bytes).getFloat
      case "int" | "integer" => ByteBuffer.wrap(bytes).getInt
      case "json" => JsonHelper.renderJson(new String(bytes), pretty = true)
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
    * @param index  the given index
    * @return the option of the value
    */
  protected def extract[T](values: Seq[T], index: Int): Option[T] = {
    if (values.length > index) Some(values(index)) else None
  }

  protected def getInputSource(params: UnixLikeArgs)(implicit rt: TxRuntimeContext): Option[MessageInputSource] = {
    params("-i") flatMap rt.getInputHandler
  }

  def getOutputSource(params: UnixLikeArgs)(implicit rt: TxRuntimeContext): Option[MessageOutputSource] = {
    params("-o") flatMap rt.getOutputHandler
  }

  /**
    * Executes a Java application via its "main" method
    * @param className the name of the class to invoke
    * @param args      the arguments to pass to the application
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
  * @author lawrence.daniels@gmail.com
  */
object Module {
  private lazy val logger = LoggerFactory.getLogger(getClass)
  val formatTypes = Seq("bytes", "char", "double", "float", "int", "json", "long", "short", "string")

  def loadUserDefinedModules(config: TxConfig, modulesFile: File, classLoader: ClassLoader): List[Module] = {
    if (modulesFile.exists()) {
      Try {
        val jsonString = Source.fromFile(modulesFile).getLines() mkString "\n"
        JsonHelper.transformTo[List[UserModule]](jsonString)
      } match {
        case Success(modules) => modules flatMap { umd =>
          logger.info(s"Loading module '${umd.name}'...")
          Try {
            val `class` = classLoader.loadClass(umd.`class`)
            val constructor = `class`.getConstructor(classOf[TxConfig])
            constructor.newInstance(config).asInstanceOf[Module]
          } match {
            case Success(module) => Option(module)
            case Failure(e) =>
              logger.warn(s"Failed to load user-defined module '${umd.name}': ${e.getMessage}")
              None
          }
        }
        case Failure(e) =>
          logger.warn(s"Unable to load modules from '${modulesFile.getName}': ${e.getMessage}")
          Nil
      }
    } else Nil
  }

  /**
    * A simple name-value pair
    * @param name  the name of the property
    * @param value the value of the property
    */
  case class NameValuePair(name: String, value: Any)

  /**
    * A user-defined module definition
    * @param name    the name of the user-defined module
    * @param `class` the class name of the user-defined module
    */
  case class UserModule(name: String, `class`: String)

}