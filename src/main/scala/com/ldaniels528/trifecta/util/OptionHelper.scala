package com.ldaniels528.trifecta.util

/**
 * Option Helper Utility Class
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
object OptionHelper {

  /**
   * Facilitates option chaining
   */
  implicit class OptionalExtensions[T](val opA: Option[T]) extends AnyVal {

    def ??(opB: => Option[T]) = if (opA.isDefined) opA else opB

    def orDie(message: String): T = opA.getOrElse(throw new IllegalStateException(message))
  }

}
