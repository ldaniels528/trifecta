package com.ldaniels528.trifecta.util

import scala.util.{Failure, Success, Try}

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

  /**
   * Converts a Success outcome into an Option of the outcome and Failure(e) to None
   * @param outcome the given Try-monad, which represents the outcome
   * @tparam T the parameter type
   */
  implicit class TryToOptionExtension[T](val outcome: Try[T]) extends AnyVal {

    def toOption: Option[T] = outcome match {
      case Success(v) => Option(v)
      case Failure(e) => None
    }

  }

}
