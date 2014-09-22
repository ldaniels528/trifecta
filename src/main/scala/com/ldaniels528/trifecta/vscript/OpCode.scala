package com.ldaniels528.trifecta.vscript

/**
 * Represents a VScript instruction (operational code)
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
trait OpCode {

  /**
   * Evaluates the operational code
   * @param scope the given [[Scope]]
   * @return the result of the operational code
   */
  def eval(implicit scope: Scope): Option[Any]

}
