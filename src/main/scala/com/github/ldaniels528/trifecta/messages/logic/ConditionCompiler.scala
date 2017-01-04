package com.github.ldaniels528.trifecta.messages.logic

import com.github.ldaniels528.trifecta.messages.codec.MessageDecoder
import com.github.ldaniels528.trifecta.messages.logic.Expressions._
import com.github.ldaniels528.trifecta.util.ParsingHelper
import com.github.ldaniels528.trifecta.util.ParsingHelper._

/**
  * Condition Compiler
  * @author lawrence.daniels@gmail.com
  */
object ConditionCompiler {

  def compile(expression: Expression, decoder: Option[MessageDecoder[_]]): Condition = {
    expression match {
      case AND(a, b) => Conditions.AND(compile(a, decoder), compile(b, decoder))
      case KEY_EQ(v) => Conditions.KeyIs(translateValue(v))
      case NOT(expr) => Conditions.NOT(compile(expr, decoder))
      case OR(a, b) => Conditions.OR(compile(a, decoder), compile(b, decoder))
      case expr =>
        decoder match {
          case Some(compiler: MessageEvaluation) => compiler.compile(expr)
          case Some(_) => throw new IllegalStateException(s"The selected decoder is not a message compiler")
          case None => throw new IllegalStateException(s"No message decoder found to support `$expr`")
        }
    }
  }

  def compile(field: String, operator: String, value: String): Expression = {
    operator.toLowerCase match {
      case "=" | "==" => EQ(field, value)
      case "!=" | "<>" => NE(field, value)
      case ">" => GT(field, value)
      case "<" => LT(field, value)
      case ">=" => GE(field, value)
      case "<=" => LE(field, value)
      case "is" =>
        if (field.equalsIgnoreCase("key")) KEY_EQ(value)
        else throw new IllegalArgumentException("Only 'key' can be used with the verb 'is'")
      case "like" => LIKE(field, value)
      case "matches" => MATCHES(field, value)
      case "unlike" => NOT(LIKE(field, value))
      case _ => throw new IllegalArgumentException(s"Illegal operator '$operator' near '$field'")
    }
  }

  /**
    * Parses a expression/condition represented by an iteration of string (e.g. Seq("x", ">", "1").iterator)
    * @param it      the iterator that represents the expression/condition
    * @param decoder the optional [[MessageDecoder]]
    * @example lastTrade < 1 and volume > 1000000
    * @return a collection of [[Condition]] objects
    */
  def parseCondition(it: Iterator[String], decoder: Option[MessageDecoder[_]]): Option[Condition] = {
    import ParsingHelper.deQuote

    var criteria: Option[Expression] = None
    while (it.hasNext) {
      val lookAhead = criteria.size + 3
      val args = it.take(lookAhead).toList
      criteria = args match {
        case List(keyword, field, operator, value) if keyword.equalsIgnoreCase("and") =>
          criteria.map(AND(_, compile(field, operator, deQuote(value))))
        case List(keyword, field, operator, value) if keyword.equalsIgnoreCase("or") =>
          criteria.map(OR(_, compile(field, operator, deQuote(value))))
        case List(field, operator, value) => Option(compile(field, operator, deQuote(value)))
        case unknown => throw new IllegalArgumentException(s"Illegal operand $unknown")
      }
    }
    criteria.map(compile(_, decoder))
  }

  private def translateValue(value: String, encoding: String = "UTF-8"): Array[Byte] = {
    if (isDottedHex(value)) parseDottedHex(value) else value.getBytes(encoding)
  }

  /**
    * SQL-like String Extensions
    * @param string the given string
    */
  implicit class SQLLikeExtensions(val string: String) extends AnyVal {

    /**
      * Performs a SQL LIKE pattern match
      * @param pattern the given pattern ("Ap%le)
      * @return true, if the string matches the patten
      */
    @inline
    def like(pattern: String): Boolean = string.matches(pattern.replaceAllLiterally("%", "(.*)"))

  }

}
