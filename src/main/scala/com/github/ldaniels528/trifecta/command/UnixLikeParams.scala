package com.github.ldaniels528.trifecta.command

import com.github.ldaniels528.trifecta.command.parser.CommandParser
import CommandParser.parseUnixLikeArgs

/**
 * Unix-Style Command Parameters
 * @param flags the given collection of flag tuple
 * @author lawrence.daniels@gmail.com
 */
case class UnixLikeParams(defaults: Seq[(String, Boolean)] = Nil, flags: Seq[(String, String)] = Nil)
  extends CommandParameters[UnixLikeArgs] {

  override def checkArgs(command: Command, args: Seq[String]) = {
    // transform the input into Unix-style arguments
    val unixArgs = transform(args)

    // there must be at least as many parameters as required arguments
    val argCount = unixArgs.args.size
    val arguments = "argument" + (if(argCount == 1) "" else "s")
    val requiredArgs = defaults.count { case (_, required) => required}
    if (argCount < requiredArgs) {
      throw new IllegalArgumentException(s"Invalid arguments ($argCount $arguments found, $requiredArgs required) - Usage: ${command.prototype}")
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
