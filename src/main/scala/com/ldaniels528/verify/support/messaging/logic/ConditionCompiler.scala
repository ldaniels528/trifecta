package com.ldaniels528.verify.support.messaging.logic

import com.ldaniels528.verify.command.CommandParser
import com.ldaniels528.verify.support.messaging.MessageDecoder
import com.ldaniels528.verify.support.messaging.logic.Operations._

/**
 * Condition Compiler
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
object ConditionCompiler {

  def compile(operation: Operation, decoder: Option[MessageDecoder[_]]): Condition = {
    operation match {
      case AND(a, b) => Conditions.AND(compile(a, decoder), compile(b, decoder))
      case KEY_EQ(v) => Conditions.KeyIs(CommandParser.parseDottedHex(v))
      case OR(a, b) => Conditions.OR(compile(a, decoder), compile(b, decoder))
      case op =>
        decoder match {
          case Some(compiler: MessageEvaluation) => compiler.compile(op)
          case Some(aDecoder) => throw new IllegalStateException(s"The selected decoder is not a message compiler")
          case None => throw new IllegalStateException("No message decoder selected")
        }
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
      case "is" =>
        if (field == "key") KEY_EQ(value)
        else throw new IllegalArgumentException("Only 'key' can be used with the verb 'is'")
      case _ => throw new IllegalArgumentException(s"Illegal operator '$operator'")
    }
  }

}
