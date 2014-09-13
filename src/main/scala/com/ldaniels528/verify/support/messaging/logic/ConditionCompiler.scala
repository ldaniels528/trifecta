package com.ldaniels528.verify.support.messaging.logic

import com.ldaniels528.verify.support.messaging.logic.Operations._

/**
 * Condition Compiler
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
object ConditionCompiler {

  def compile(operation: Operation, compiler: MessageComparison): Condition = {
    operation match {
      case AND(a, b) => Conditions.AND(compile(a, compiler), compile(b, compiler))
      case OR(a, b) => Conditions.OR(compile(a, compiler), compile(b, compiler))
      case op => compiler.compile(op)
    }
  }

  def compile(field: String, operator: String, value: String): Operation = {
    operator match {
      case "==" => EQ(field, value)
      case "!=" => NE(field, value)
      case ">" => GT(field, value)
      case "<" => LT(field, value)
      case ">=" => GE(field, value)
      case "<=" => LE(field, value)
      case _ => throw new IllegalArgumentException(s"Illegal operator '$operator'")
    }
  }

}
