package com.ldaniels528.trifecta.util

import scala.language.reflectiveCalls

/**
 * Resource Helper Utility Class
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
object ResourceHelper {

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

}
