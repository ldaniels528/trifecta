package com.ldaniels528.verify.command

import com.ldaniels528.verify.modules.Module

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

