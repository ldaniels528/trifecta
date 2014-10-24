package com.ldaniels528.trifecta.util

import java.io.{ByteArrayOutputStream, PrintStream}
import java.util.Properties

import scala.collection.JavaConverters._
import scala.concurrent.duration.FiniteDuration
import scala.language._
import scala.util.{Failure, Success, Try}

/**
 * Trifecta Utilities
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
object TxUtils {

  /**
   * Indicates whether the string is blank (null or empty or whitespace)
   */
  def isBlank(s: String) = (s == null) || (s.trim.length == 0)

  /**
   * Attempts the given <tt>task</tt> up to <tt>count</tt> times before
   * throwing the exception produced by the final attempt.
   */
  def attempt[S](count: Int, delay: FiniteDuration)(task: => S): S = {

    def invoke(task: => S, attempts: Int): S = {
      Try(task) match {
        case Success(outcome) => outcome
        case Failure(e) =>
          if (attempts < count) {
            Thread.sleep(delay)
            invoke(task, attempts + 1)
          } else throw new IllegalStateException(s"Action failed $count times: ${e.getMessage}", e)
      }
    }

    invoke(task, 0)
  }

  /**
   * Executes a block; capturing any generated output to STDOUT and/or STDERR
   * @param initialSize the initial size of the buffer
   * @param block the code block to execute
   * @tparam S the return type of the code block
   * @return byte arrays representing STDOUT and STDERR and the return value of the block
   */
  def sandbox[S](initialSize: Int = 256)(block: => S): (Array[Byte], Array[Byte], S) = {
    // capture standard output & error and create my own buffers
    val out = System.out
    val err = System.err
    val outBuf = new ByteArrayOutputStream(initialSize)
    val errBuf = new ByteArrayOutputStream(initialSize)

    // redirect standard output and error to my own buffers
    System.setOut(new PrintStream(outBuf))
    System.setErr(new PrintStream(errBuf))

    // execute the block; capturing standard output & error
    val result = try block finally {
      System.setOut(out)
      System.setErr(err)
    }
    (outBuf.toByteArray, errBuf.toByteArray, result)
  }

  /**
   * Syntactic sugar for timeout expressions
   */
  implicit def duration2Long(d: FiniteDuration): Long = d.toMillis

  /**
   * Automatically closes a resource after completion of a code block
   */
  implicit class AutoClose[T <: {def close()}](val resource: T) extends AnyVal {

    def use[S](block: T => S): S = try block(resource) finally resource.close()

  }

  /**
   * Automatically closes a resource after completion of a code block
   * @author Lawrence Daniels <lawrence.daniels@gmail.com>
   */
  implicit class AutoDisconnect[T <: {def disconnect()}](val resource: T) extends AnyVal {

    def use[S](block: T => S): S = try block(resource) finally resource.disconnect()

  }

  /**
   * Automatically closes a resource after completion of a code block
   */
  implicit class AutoShutdown[T <: {def shutdown()}](val resource: T) extends AnyVal {

    def use[S](block: T => S): S = try block(resource) finally resource.shutdown()

  }

  /**
   * Facilitates option chaining
   */
  implicit class OptionalMagic[T](val opA: Option[T]) extends AnyVal {

    def ??(opB: => Option[T]) = if (opA.isDefined) opA else opB

  }

  /**
   * Properties Conversion
   */
  implicit class PropertiesConversion[T <: Object](val m: Map[String, T]) extends AnyVal {

    import java.util.Properties

    def toProps: Properties = {
      val p = new Properties()
      p.putAll(m.asJava)
      p
    }
  }

  /**
   * Syntactic Sugar for Properties object
   */
  implicit class PropertiesMagic(val props: Properties) extends AnyVal {

    def asOpt[T](key: String): Option[T] = {
      Option(props.get(key)) map (_.asInstanceOf[T])
    }

    def getOrElse(key: String, default: => String): String = {
      Option(props.getProperty(key)).getOrElse(default)
    }

  }

  /**
   * Convenience method for extracted the suffix of a string based on a matched prefix
   * @param src the given source string
   */
  implicit class StringMagic(val src: String) extends AnyVal {

    def extractProperty(prefix: String): Option[String] = {
      if (src.startsWith(prefix)) Option(src.substring(prefix.length)) else None
    }

  }

}
