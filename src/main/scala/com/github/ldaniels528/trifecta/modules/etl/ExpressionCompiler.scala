package com.github.ldaniels528.trifecta.modules.etl

import com.github.ldaniels528.commons.helpers.OptionHelper._
import com.github.ldaniels528.trifecta.modules.etl.io.Scope

import scala.annotation.tailrec
import scala.util.{Failure, Success, Try}

/**
  * Handlebar Expression Compiler
  * @author lawrence.daniels@gmail.com
  */
object ExpressionCompiler {

  /**
    * Parses and compiles handlebar expressions
    * @param expr  the given expression string (e.g. "{{ csv_body.date | date:'yyyyMMdd' }}")
    * @param scope the given [[Scope scope]]
    * @return the result of the evaluated expression
    */
  def handlebars(expr: String)(implicit scope: Scope) = {
    if (isFullReplacement(expr)) handlebarsFull(expr) else Option(handlebarsPartial(expr))
  }

  private def isFullReplacement(expr: String): Boolean = {
    expr.indexOf("{{") == 0 && expr.indexOf("}}") == expr.length - 2 && {
      var count = 0
      var index = 0
      do {
        index = expr.indexOf("{{", index)
        if (index != -1) {
          count += 1
          index += 2
        }
      } while (index != -1)
      count == 1
    }
  }

  private def handlebarsFull(expr: String)(implicit scope: Scope): Option[Any] = {
    val start = expr.indexOf("{{")
    val end = expr.indexOf("}}", start)
    if (start == -1 || end == -1) None
    else {
      val reference = expr.substring(start + 2, end - 1).trim
      compile(reference)
    }
  }

  private def handlebarsPartial(expr: String)(implicit scope: Scope): String = {
    val sb = new StringBuilder(expr)
    var lastIndex = -1
    do {
      val start = sb.indexOf("{{", lastIndex)
      val end = sb.indexOf("}}", start)
      if (start != -1 && end > start) {
        val reference = sb.substring(start + 2, end - 1).trim
        sb.replace(start, end + 2, compile(reference).map(_.toString).getOrElse(""))
        lastIndex = start
      }
      else lastIndex = -1

    } while (lastIndex != -1 && lastIndex < sb.length)

    sb.toString()
  }

  private def compile(code: String)(implicit scope: Scope) = {
    // are there filters definitions? (e.g. "csv_body.date | date:'yyyyMMdd'")
    val (variable, filterDefs) = parseNameAndArgs(code.split("[|]"))

    // lookup the filters
    val filtersWithArgs = filterDefs map { filterDef =>
      val (name, args) = parseNameAndArgs(filterDef.split("[:]"))
      (scope.findFilter(name) orDie s"Filter '$name' not found", args map unquote)
    }

    // retrieve the value, and update with the filters
    filtersWithArgs.foldLeft(scope.find(variable)) { case (value, (filter, args)) =>
      filter.execute(value, args)
    }
  }

  private def parseNameAndArgs(values: Seq[String]) = {
    values.map(_.trim).toList match {
      case name :: Nil => (name, Nil)
      case name :: args => (name, args)
      case Nil =>
        throw new IllegalStateException("Empty arguments list for filter")
    }
  }

  private def unquote(s: String) = if (s.startsWith("\"") && s.endsWith("\"")) s.drop(1).dropRight(1) else s

  @tailrec
  private def unwrap(result: Any): String = {
    result match {
      case e: Either[_, _] => e match {
        case Left(value) => unwrap(value)
        case Right(value) => unwrap(value)
      }
      case o: Option[_] => o match {
        case Some(value) => unwrap(value)
        case None => ""
      }
      case t: Try[_] => t match {
        case Success(value) => unwrap(value)
        case Failure(e) => e.getMessage
      }
      case value => value.toString
    }
  }

}
