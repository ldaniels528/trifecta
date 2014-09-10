package com.ldaniels528.verify.modules

import com.ldaniels528.verify.modules.CommandParser._

import scala.language.existentials

/**
 * Represents a Shell command
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
case class Command(module: Module,
                   name: String,
                   fx: UnixLikeArgs => Any,
                   params: CommandParameters[_],
                   help: String = "",
                   promptAware: Boolean = false,
                   undocumented: Boolean = false) {

  def prototype: String = params.prototypeOf(this)

  override def toString = prototype

}

/**
 * Represents the parameters of a Shell command
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
sealed trait CommandParameters[T] {

  def checkArgs(command: Command, args: Seq[String]): Unit

  def prototypeOf(command: Command): String

  def transform(args: Seq[String]): T

}

/**
 * Simple Command Parameters
 * @param required the sequence of required parameters
 * @param optional the sequence of optional parameters
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
case class SimpleParams(required: Seq[String] = Nil, optional: Seq[String] = Nil)
  extends CommandParameters[Seq[String]] {

  override def checkArgs(command: Command, args: Seq[String]) {
    if (args.length < required.size || args.length > required.size + optional.size) {
      throw new IllegalArgumentException(s"Usage: ${command.prototype}")
    }
  }

  override def prototypeOf(command: Command): String = {
    val items = optional.map(s => s"[$s]").toList ::: required.toList ::: command.name :: Nil
    items.reverse mkString " "
  }

  override def transform(args: Seq[String]): Seq[String] = args

}

/**
 * Unix-Style Command Parameters
 * @param flags the given collection of flag tuple
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
case class UnixLikeParams(defaults: Seq[(String, Boolean)] = Nil, flags: Seq[(String, String)] = Nil)
  extends CommandParameters[UnixLikeArgs] {

  override def checkArgs(command: Command, args: Seq[String]) = {
    val unixArgs = CommandParser.parseUnixLikeArgs(args)
    unixArgs.flags foreach { case (flag, _) =>
      if (!flags.contains(flag)) throw new IllegalArgumentException(s"Invalid flag '$flag' - Usage: ${command.prototype}")
    }
  }

  override def prototypeOf(command: Command): String = {
    val items = defaults.map { case (param, required) => if (required) param else s"[$param]"}.reverse.toList :::
      (flags.toList map { case (flag, name) => s"[$flag $name]"}) ::: command.name :: Nil
    items.reverse mkString " "
  }

  override def transform(args: Seq[String]): UnixLikeArgs = parseUnixLikeArgs(args)

}