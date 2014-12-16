package com.ldaniels528.trifecta.util

/**
 * Either Helper Utility Class
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
object EitherHelper {

  implicit class EitherExtensions[A, B](val either: Either[A, B]) extends AnyVal {

    def toLeftOption: Option[A] = either match {
      case Left(v) => Option(v)
      case _ => None
    }

    def toRightOption: Option[B] = either match {
      case Right(v) => Option(v)
      case _ => None
    }

  }

}
