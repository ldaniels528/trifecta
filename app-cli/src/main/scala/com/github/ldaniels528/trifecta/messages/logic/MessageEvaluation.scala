package com.github.ldaniels528.trifecta.messages.logic

import com.github.ldaniels528.trifecta.messages.logic.Expressions._

/**
 * Message Evaluation
 * @author lawrence.daniels@gmail.com
 */
trait MessageEvaluation {

  /**
   * Compiles the given operation into a condition
   * @param operation the given operation
   * @return a condition
   */
  def compile(operation: Expression): Condition

}
