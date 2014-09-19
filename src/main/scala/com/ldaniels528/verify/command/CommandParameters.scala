package com.ldaniels528.verify.command

/**
 * Represents the parameters of a Shell command
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
trait CommandParameters[T] {

  def checkArgs(command: Command, args: Seq[String]): Unit

  def prototypeOf(command: Command): String

  def transform(args: Seq[String]): T

}
