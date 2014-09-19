package com.ldaniels528.verify.command

import CommandParser.parseUnixLikeArgs

/**
 * Unix-Style Command Parameters
 * @param flags the given collection of flag tuple
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
case class UnixLikeParams(defaults: Seq[(String, Boolean)] = Nil, flags: Seq[(String, String)] = Nil)
  extends CommandParameters[UnixLikeArgs] {

  override def checkArgs(command: Command, args: Seq[String]) = {
    // transform the input into Unix-style arguments
    val unixArgs = transform(args)

    // there must be at least as many parameters as required arguments
    if (defaults.count { case (_, required) => required} < unixArgs.args.size) {
      throw new IllegalArgumentException(s"Invalid arguments - Usage: ${command.prototype}")
    }

    // required flags are mandatory
    val myFlags = Map(flags: _*)
    unixArgs.flags foreach { case (flag, _) =>
      if (!myFlags.contains(flag)) throw new IllegalArgumentException(s"Invalid flag '$flag' - Usage: ${command.prototype}")
    }
  }

  override def prototypeOf(command: Command): String = {
    val items = defaults.map { case (param, required) => if (required) param else s"[$param]"}.reverse.toList :::
      (flags.toList map { case (flag, name) => s"[$flag $name]"}) ::: command.name :: Nil
    items.reverse mkString " "
  }

  override def transform(args: Seq[String]): UnixLikeArgs = parseUnixLikeArgs(args)

}
