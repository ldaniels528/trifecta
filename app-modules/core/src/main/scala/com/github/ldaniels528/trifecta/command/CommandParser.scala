package com.github.ldaniels528.trifecta.command

/**
  * Command Parser
  * @author lawrence.daniels@gmail.com
  */
object CommandParser {
  private val SYMBOLS = Set('!', '?', '&')

  /**
    * Parses the given input string into tokens
    * @param input the given user input to parse
    */
  def parseTokens(input: String): List[String] = {
    val sb = new StringBuilder()
    var inDoubleQuotes = false
    var inSingleQuotes = false
    var inBackTicks = false
    var jsonLevel = 0

    def closeOut(done: => Boolean) = {
      if (done) {
        val text = sb.toString()
        sb.clear()
        Some(text)
      }
      else None
    }

    def inQuotes = inBackTicks || inDoubleQuotes || inSingleQuotes

    // extract the tokens
    val list = input.foldLeft[List[String]](Nil) { (list, ch) =>
      val result: Option[String] = ch match {
        // JSON open
        case c if c == '{' && !inQuotes =>
          sb += c
          jsonLevel += 1
          None

        // JSON close
        case c if c == '}' && !inQuotes =>
          sb += c
          jsonLevel -= 1
          closeOut(jsonLevel == 0)

        // JSON character inclusion
        case c if jsonLevel > 0 =>
          sb += c
          None

        // double-quoted text
        case '"' if !inBackTicks && !inSingleQuotes =>
          inDoubleQuotes = !inDoubleQuotes
          closeOut(!inDoubleQuotes)

        // single-quoted text
        case '\'' if !inBackTicks && !inDoubleQuotes =>
          inSingleQuotes = !inSingleQuotes
          closeOut(!inSingleQuotes)

        // back-tick quoted text
        case c if c == '`' && !inDoubleQuotes && !inSingleQuotes =>
          sb += c
          inBackTicks = !inBackTicks
          closeOut(!inBackTicks)

        // quoted character inclusion
        case c if inQuotes =>
          sb += c
          None

        // symbol (unquoted)?
        case c if SYMBOLS.contains(c) =>
          val s = sb.toString()
          sb.clear()
          if (s.isEmpty) Option(String.valueOf(c))
          else {
            sb += c
            Option(s)
          }

        // space (unquoted)?
        case c if c == ' ' =>
          closeOut(sb.nonEmpty)

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

  /**
    * Parses the given input string into tokens
    * @return the argument list
    */
  def parseUnixLikeArgs(input: String): UnixLikeArgs = parseUnixLikeArgs(parseTokens(input))

  /**
    * Parses the given items (e.g. ["-c", "-f", "myfile"]) into an argument list (e.g. ["-c" -> None, "-f" -> Some("myfile")])
    * @param items the given array of items
    * @return the argument list
    */
  def parseUnixLikeArgs(items: Seq[String]): UnixLikeArgs = {
    val args = if (items.nonEmpty) items.tail else Nil
    val result = args.foldLeft[Accumulator](Accumulator()) { case (acc: Accumulator, item) =>
      // is the item flag?
      if (item.startsWith("-") && item.length > 1 && !item.matches("-?\\d+")) {
        if (acc.flag.isDefined) {
          acc.flag foreach (flag => acc.flags = (flag -> None) :: acc.flags)
        }
        acc.flag = Option(item)
      }

      // is a flag already defined?
      else if (acc.flag.isDefined) {
        acc.flag foreach (flag => acc.flags = (flag -> Option(item)) :: acc.flags)
        acc.flag = None
      }

      // must be an argument
      else acc.args = item :: acc.args
      acc
    }

    val flags = result.flag map (flag => flag -> None :: result.flags) getOrElse result.flags
    UnixLikeArgs(items.headOption, result.args.reverse, Map(flags: _*))
  }

  private case class Accumulator(var args: List[String] = Nil,
                                 var flags: List[(String, Option[String])] = Nil,
                                 var flag: Option[String] = None)

}