package com.github.ldaniels528.commons.helpers

/**
 * String Helper Utility Class
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
object StringHelper {

  implicit class ByteArrayExtensions(val bytes: Array[Byte]) extends AnyVal {

    def isPrintable: Boolean = bytes forall (b => b >= 32 && b <= 127)

  }

  /**
   * Convenience method for extracted the suffix of a string based on a matched prefix
   * @param src the given source string
   */
  implicit class StringExtensions(val src: String) extends AnyVal {

    def extractProperty(prefix: String): Option[String] = {
      if (src.startsWith(prefix)) Option(src.substring(prefix.length)) else None
    }

    def indexOptionOf(s: String): Option[Int] = {
      val index = src.indexOf(s)
      if (index == -1) None else Some(index)
    }

    def lastIndexOptionOf(s: String): Option[Int] = {
      val index = src.lastIndexOf(s)
      if (index == -1) None else Some(index)
    }

  }

}
