package com.ldaniels528.verify.util

/**
 * Verify Command Parser
 * @author lawrence.daniels@gmail.com
 */
object CommandParser {

  /**
   * Parses the given input string into tokens
   */
  def parse(input: String): Seq[String] = {
    val sb = new StringBuilder()
    var inQuotes = false

    // extract the tokens
    val list = input.foldLeft[List[String]](Nil) { (list, c) =>
      val result = c match {
        case '"' =>
          inQuotes = !inQuotes
          None
        case c if c == ' ' && !inQuotes =>
          if (sb.nonEmpty) {
            val s = sb.toString
            sb.clear()
            Some(s)
          } else None
        case c =>
          sb += c
          None
      }

      result map (_ :: list) getOrElse list
    }

    // add the last token
    (if (sb.nonEmpty) sb.toString :: list else list).reverse
  }

}