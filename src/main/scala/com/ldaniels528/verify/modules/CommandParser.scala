package com.ldaniels528.verify.modules

/**
 * Verify Command Parser
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
object CommandParser {
  private val SYMBOLS = Set('!', '@', '?', '&')

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

  /**
   * Parses the given items (e.g. ["-c", "-f", "myfile"]) into an argument list (e.g. ["-c" -> None, "-f" -> Some("myfile")])
   * @param items the given array of items
   * @return the argument list
   */
  def parseUnixLikeArgs(items: Seq[String]): UnixLikeArgs = {
    val result = items.foldLeft[Accumulator](Accumulator()) { case (acc: Accumulator, item) =>
      // is the item flag?
      if (item.startsWith("-")) {
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
    UnixLikeArgs(result.args.reverse, Map(flags: _*))
  }

  private case class Accumulator(var args: List[String] = Nil,
                                 var flags: List[(String, Option[String])] = Nil,
                                 var flag: Option[String] = None)

  /**
   * Represents a set of Unix-style parameters (e.g. "kget -a schema -f outfile.txt shocktrades.quotes.csv 0 165 -b")
   * @param args the given arguments (e.g. ("kget", "shocktrades.quotes.csv", "0", "165"))
   * @param flags the flag arguments (e.g. ("-a" -> "schema", "-f" -> "outfile.txt", "-b" -> None))
   */
  case class UnixLikeArgs(args: List[String], flags: Map[String, Option[String]] = Map.empty) {

    def apply(index: Int): String = args(index)

    def apply(flag: String) = flags.get(flag).flatten

  }

}