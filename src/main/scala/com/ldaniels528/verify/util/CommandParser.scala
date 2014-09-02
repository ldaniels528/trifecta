package com.ldaniels528.verify.util

/**
 * Verify Command Parser
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
object CommandParser {
  private val SYMBOLS = Set('!', '@', '?', '&')

  /**
   * Parses the given items (e.g. ["-c", "-f", "myfile"]) into an argument list (e.g. ["-c" -> Nil, "-f" -> "myfile"])
   * @param items the given array of items
   * @return the argument list
   */
  def parseArgs(items: Seq[String]): List[(String, List[String])] = {
    val (args, lastArg) = items.map(_.trim).foldRight[(List[(String, List[String])], List[String])]((Nil, Nil)) {
      case (item, (props, list)) =>
        if (item.startsWith("-")) ((item -> list) :: props, Nil)
        else if (item.trim.length > 0) (props, item :: list)
        else (props, list)
    }

    // return the argument list
    if (lastArg.nonEmpty) ("" -> lastArg) :: args else args
  }

  /**
   * Parses the given input string into tokens
   */
  def parse(input: String): Seq[String] = {
    val sb = new StringBuilder()
    var inQuotes = false

    // extract the tokens
    val list = input.foldLeft[List[String]](Nil) { (list, ch) =>
      val result: Option[String] = ch match {
        // symbol (unquoted)?
        case c if SYMBOLS.contains(c) && !inQuotes =>
          val s = sb.toString()
          sb.clear()
          if (s.isEmpty) Some(String.valueOf(c))
          else {
            sb += c
            Some(s)
          }

        // quoted text
        case '"' =>
          inQuotes = !inQuotes
          None

        // space (unquoted)?
        case c if c == ' ' && !inQuotes =>
          if (sb.nonEmpty) {
            val s = sb.toString()
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