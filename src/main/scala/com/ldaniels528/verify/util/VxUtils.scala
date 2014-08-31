package com.ldaniels528.verify.util

import java.io.{ByteArrayOutputStream, PrintStream}

import scala.concurrent.duration.FiniteDuration
import scala.language._
import scala.util.{Failure, Success, Try}

/**
 * Verify Utilities
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
object VxUtils {

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
   * Properties Conversion
   * @author Lawrence Daniels <lawrence.daniels@gmail.com>
   */
  implicit class PropertiesConversion[T <: Object](m: Map[String, T]) {

    import java.util.Properties

import scala.collection.JavaConversions.mapAsJavaMap

    def toProps: Properties = {
      val p = new Properties()
      p.putAll(mapAsJavaMap(m))
      p
    }
  }

  /**
   * Facilitates option chaining
   * @author Lawrence Daniels <lawrence.daniels@gmail.com>
   */
  implicit class OptionalMagic[T](opA: Option[T]) {

    def ??(opB: => Option[T]) = if (opA.isDefined) opA else opB

  }

  /**
   * Automatically closes a resource after completion of a code block
   * @author Lawrence Daniels <lawrence.daniels@gmail.com>
   */
  implicit class AutoClose[T <: {def close()}](resource: T) {

    def use[S](block: T => S): S = try block(resource) finally resource.close()

  }

  /**
   * Automatically closes a resource after completion of a code block
   * @author Lawrence Daniels <lawrence.daniels@gmail.com>
   */
  implicit class AutoDisconnect[T <: {def disconnect()}](resource: T) {

    def use[S](block: T => S): S = try block(resource) finally resource.disconnect()

  }

  /**
   * Automatically closes a resource after completion of a code block
   * @author Lawrence Daniels <lawrence.daniels@gmail.com>
   */
  implicit class AutoShutdown[T <: {def shutdown()}](resource: T) {

    def use[S](block: T => S): S = try block(resource) finally resource.shutdown()

  /**
   * Syntactic Sugar for Properties object
   */
  implicit class PropertiesMagic(props: Properties) {

    def asOpt[T](key: String): Option[T] = {
      Option(props.get(key)) map (_.asInstanceOf[T])
    }
  }

}
