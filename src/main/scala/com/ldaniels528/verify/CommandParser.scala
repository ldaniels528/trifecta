package com.ldaniels528.verify

/**
 * Verify Command Parser
 * @author lawrence.daniels@gmail.com
 */
object CommandParser {
  private val SYMBOLS = Set('!', '@', '?')

  /**
   * Parses the given input string into tokens
   */
  def parse(input: String): Seq[String] = {
    val sb = new StringBuilder()
    var inQuotes = false

    // extract the tokens
    val list = input.foldLeft[List[String]](Nil) { (list, c) =>
      val result: Option[String] = c match {
        // symbol (unquoted)?
        case c if SYMBOLS.contains(c) && !inQuotes =>
          val s = sb.toString
          sb.clear()
          if (s.isEmpty) Some(String.valueOf(c))
          else {
            sb += c
            Some(s)
          }

        // quote
        case '"' =>
          inQuotes = !inQuotes
          None

        // space (unquoted)?
        case c if c == ' ' && !inQuotes =>
          if (sb.nonEmpty) {
            val s = sb.toString
            sb.clear()
            Some(s)
          } else None

        // any other character
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