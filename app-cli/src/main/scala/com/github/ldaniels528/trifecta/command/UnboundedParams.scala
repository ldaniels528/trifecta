package com.github.ldaniels528.trifecta.command

/**
 * Represents a set of Unbounded Parameters
 * @author lawrence.daniels@gmail.com
 */
case class UnboundedParams(minimum: Int) extends CommandParameters[Seq[String]] {

  /**
   * Validates that the given tokens are acceptable as arguments for the given command
   * @param command the given command
   * @param tokens the given tokens
   */
  override def checkArgs(command: Command, tokens: Seq[String]): Unit = {
    if(tokens.size < minimum) {
      throw new IllegalArgumentException(s"Usage: ${command.prototype}")
    }
  }

  /**
   * Returns a usage prototype for the given command
   * @param command the given command
   * @return a usage prototype for the given command (e.g. "zget [-t type] key")
   */
  override def prototypeOf(command: Command): String = s"${command.name} ..."

  /**
   * Transform the given tokens into a typed argument list
   * @param tokens the given tokens
   * @return a typed argument list
   */
  override def transform(tokens: Seq[String]): Seq[String] = tokens

}
