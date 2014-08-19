package com.ldaniels528.verify.util

import scala.concurrent.duration.FiniteDuration
import scala.language._
import scala.util.{Failure, Success, Try}

/**
 * Verify Utilities
 * @author lawrence.daniels@gmail.com
 */
object VerifyUtils {

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
   * Syntactic sugar for timeout expressions
   */
  implicit def duration2Long(d: FiniteDuration): Long = d.toMillis

  /**
   * Properties Conversion
   * @author lawrence.daniels@gmail.com
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
   * @author lawrence.daniels@gmail.com
   */
  implicit class OptionalMagic[T](opA: Option[T]) {

    def ??(opB: Option[T]) = if (opA.isDefined) opA else opB

    def ??(opB: T) = if (opA.isDefined) opA else Option(opB)

  }

  /**
   * Automatically closes a resource after completion of a code block
   * @author lawrence.daniels@gmail.com
   */
  implicit class AutoClose[T <: {def close()}](resource: T) {

    def use[S](block: T => S): S = try block(resource) finally resource.close()

  }

  /**
   * Automatically closes a resource after completion of a code block
   * @author lawrence.daniels@gmail.com
   */
  implicit class AutoDisconnect[T <: {def disconnect()}](resource: T) {

    def use[S](block: T => S): S = try block(resource) finally resource.disconnect()

  }

  /**
   * Automatically closes a resource after completion of a code block
   * @author lawrence.daniels@gmail.com
   */
  implicit class AutoShutdown[T <: {def shutdown()}](resource: T) {

    def use[S](block: T => S): S = try block(resource) finally resource.shutdown()

  }

}
