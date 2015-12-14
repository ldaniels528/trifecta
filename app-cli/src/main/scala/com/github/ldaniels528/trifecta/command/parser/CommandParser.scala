package com.github.ldaniels528.trifecta.command.parser

import com.github.ldaniels528.trifecta.command.UnixLikeArgs

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
  def parseTokens(input: String): Seq[String] = {
    val sb = new StringBuilder()
    var inQuotes = false
    var inBackTicks = false
    var jsonLevel = 0

    // extract the tokens
    val list = input.foldLeft[List[String]](Nil) { (list, ch) =>
      val result: Option[String] = ch match {
        // back ticks (start)
        case c if c == '`' && !inQuotes =>
          inBackTicks = !inBackTicks
          sb += c
          if(!inBackTicks) {
            val s = sb.toString()
            sb.clear()
            Option(s)
          }
          else None

        // back ticks (inclusion)
        case c if inBackTicks =>
          sb += c
          None

        // JSON open
        case c if c == '{' && !inQuotes =>
          sb += c
          jsonLevel += 1
          None

        // JSON close
        case c if c == '}' && !inQuotes =>
          sb += c
          jsonLevel -= 1
          if (jsonLevel > 0) None
          else {
            jsonLevel = 0
            val s = sb.toString()
            sb.clear()
            Option(s)
          }

        // any character inside of JSON braces
        case c if jsonLevel > 0 =>
          sb += c
          None

        // is it a label?
        /*
        case c if c == ':' && !inQuotes =>
          sb += c
          val s = sb.toString()
          sb.clear()
          Option(s)*/

        // symbol (unquoted)?
        case c if SYMBOLS.contains(c) && !inQuotes =>
          val s = sb.toString()
          sb.clear()
          if (s.isEmpty) Option(String.valueOf(c))
          else {
            sb += c
            Option(s)
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
            Option(s)
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

  /**
   * Indicates whether the given string is hexadecimal dot-notation
   * @param value the given string value
   * @return true, if the string is hexadecimal dot-notation (e.g. "de.ad.be.ef.ca.fe.ba.be")
   */
  def isDottedHex(value: String): Boolean = {
    value.split("[.]").forall(_.matches( """[0-9a-fA-F]{2}"""))
  }

  /**
   * Converts a binary string to a byte array
   * @param dottedHex the given binary string (e.g. "de.ad.be.ef.00")
   * @return a byte array
   */
  def parseDottedHex(dottedHex: String): Array[Byte] = dottedHex.split("[.]") map (Integer.parseInt(_, 16)) map (_.toByte)

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