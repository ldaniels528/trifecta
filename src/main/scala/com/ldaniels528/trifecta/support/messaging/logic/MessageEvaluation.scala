package com.ldaniels528.trifecta.support.messaging.logic

import com.ldaniels528.trifecta.support.messaging.logic.Expressions._

/**
 * Message Evaluation
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
trait MessageEvaluation {

  /**
   * Compiles the given operation into a condition
   * @param operation the given operation
   * @return a condition
   */
  def compile(operation: Expression): Condition

}
