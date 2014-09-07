package com.ldaniels528.verify.modules

/**
 * Represents a Shell command
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
case class Command(module: Module,
                   name: String,
                   fx: Seq[String] => Any,
                   params: CommandParameters,
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
sealed trait CommandParameters {

  def checkArgs(command: Command, args: Seq[String]): Unit

  def prototypeOf(command: Command): String

}

/**
 * Simple Command Parameters
 * @param required the sequence of required parameters
 * @param optional the sequence of optional parameters
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
case class SimpleParams(required: Seq[String] = Seq.empty, optional: Seq[String] = Seq.empty)
  extends CommandParameters {

  override def checkArgs(command: Command, args: Seq[String]) {
    if (args.length < required.size || args.length > required.size + optional.size) {
      throw new IllegalArgumentException(s"Usage: ${command.prototype}")
    }
  }

  override def prototypeOf(command: Command): String = {
    val requiredParams = (required map (s => s"<$s>")).mkString(" ")
    val optionalParams = (optional map (s => s"<$s>")).mkString(" ")
    s"${command.name} $requiredParams ${if (optionalParams.nonEmpty) s"[$optionalParams]" else ""}"
  }

}

/**
 * Unix-Style Command Parameters
 * @param flags the given collection of flag tuple
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
case class UnixLikeParams(required: Seq[String] = Nil, flags: Seq[(String, String)] = Nil)
  extends CommandParameters[List[(String, List[String])]] {

  override def checkArgs(command: Command, args: Seq[String]) = ()

  override def prototypeOf(command: Command): String = {
    val params = flags.foldLeft[List[String]](Nil) { case (list, (flag, desc)) =>
      desc :: flag :: list
    }
    params.reverse.mkString(" ")
  }

  override def transform(args: Seq[String]): List[(String, List[String])] = parseArgs(args)

}