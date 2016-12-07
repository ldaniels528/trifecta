package com.github.ldaniels528.trifecta.messages.logic

import com.github.ldaniels528.trifecta.messages.BinaryMessage
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

  /**
    * Evaluates the message; returning the resulting field and values
    * @param msg the given [[BinaryMessage binary message]]
    * @param fields the given subset of fields to return
    * @return the mapping of fields and values
    */
  def evaluate(msg: BinaryMessage, fields: Seq[String]): Map[String, Any]

}

/**
  * Message Evaluation Companion
  * @author lawrence.daniels@gmail.com
  */
object MessageEvaluation {

  /**
    * Field name selection extension
    * @param fields the given collection of field names
    */
  implicit class FieldNameSelectionExtension(val fields: Seq[String]) extends AnyVal {

    @inline
    def isAllFields: Boolean = fields.contains("*")

  }

}