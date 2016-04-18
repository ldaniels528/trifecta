package com.github.ldaniels528.trifecta.command

/**
 * Represents a set of Unix-style parameters (e.g. "kget -a schema -f outfile.txt shocktrades.quotes.csv 0 165 -b")
 * @param commandName the given command name (e.g. "kget")
 * @param args the given arguments (e.g. ("shocktrades.quotes.csv", "0", "165"))
 * @param flags the flag arguments (e.g. ("-a" -> "schema", "-f" -> "outfile.txt", "-b" -> None))
 */
case class UnixLikeArgs(commandName: Option[String], args: List[String], flags: Map[String, Option[String]] = Map.empty) {

  def apply(index: Int): String = args(index)

  def apply(flag: String) = flags.get(flag).flatten

  def apply(flag: String, defaultValue: String) = flags.get(flag).flatten getOrElse defaultValue

  def contains(flag: String) = flags.get(flag).isDefined

}