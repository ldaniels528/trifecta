package com.github.ldaniels528.trifecta.command

/**
 * Represents the parameters of a Shell command
 * @author lawrence.daniels@gmail.com
 */
trait CommandParameters[T] {

  /**
   * Validates that the given tokens are acceptable as arguments for the given command
   * @param command the given command
   * @param tokens the given tokens
   */
  def checkArgs(command: Command, tokens: Seq[String]): Unit

  /**
   * Returns a usage prototype for the given command
   * @param command the given command
   * @return a usage prototype for the given command (e.g. "zget [-t type] key")
   */
  def prototypeOf(command: Command): String

  /**
   * Transform the given tokens into a typed argument list
   * @param tokens the given tokens
   * @return a typed argument list
   */
  def transform(tokens: Seq[String]): T

}
