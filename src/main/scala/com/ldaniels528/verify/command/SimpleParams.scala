package com.ldaniels528.verify.command

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
    val items: List[String] = command.name :: optional.map(s => s"[$s]").toList ::: required.toList ::: Nil
    items mkString " "
  }

  override def transform(args: Seq[String]): Seq[String] = args

}