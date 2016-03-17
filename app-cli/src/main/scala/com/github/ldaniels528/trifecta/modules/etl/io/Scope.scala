package com.github.ldaniels528.trifecta.modules.etl.io

import java.util.UUID

import com.github.ldaniels528.trifecta.modules.etl.ExpressionCompiler
import com.github.ldaniels528.trifecta.modules.etl.io.Scope.DynamicValue
import com.github.ldaniels528.trifecta.modules.etl.io.filters.Filter

import scala.collection.concurrent.TrieMap

/**
  * Represents a runtime scope
  * @author lawrence.daniels@gmail.com
  */
class Scope(values: (String, Any)*) {
  private val filters = TrieMap[String, Filter]()
  private val variables = TrieMap[String, Any](values: _*)
  private val resources = TrieMap[UUID, Any]()

  ///////////////////////////////////////////////////////////////////////
  //    Filter-specific Methods
  ///////////////////////////////////////////////////////////////////////

  def addFilter(name: String, filter: Filter) = filters += name -> filter

  def findFilter(name: String) = filters.get(name)

  ///////////////////////////////////////////////////////////////////////
  //    Property-specific Methods
  ///////////////////////////////////////////////////////////////////////

  def ++=(values: Seq[(String, Any)]) = variables ++= values

  def +=(tuple: (String, Any)) = variables += tuple

  def evaluateAsString(expression: String) = ExpressionCompiler.handlebars(expression)(this) map(_.toString) getOrElse ""

  def evaluate(expression: String) = ExpressionCompiler.handlebars(expression)(this)

  def find(name: String): Option[Any] = {
    variables.get(name) map {
      case fx: DynamicValue => fx()
      case value => value
    }
  }

  def getOrElseUpdate[T](key: String, value: => T): T = {
    variables.getOrElseUpdate(key, value).asInstanceOf[T]
  }

  def toSeq = variables.toSeq

  ///////////////////////////////////////////////////////////////////////
  //    Resource-specific Methods
  ///////////////////////////////////////////////////////////////////////

  def createResource[T](id: UUID, action: => T) = resources.getOrElseUpdate(id, action).asInstanceOf[T]

  def discardResource[T](id: UUID) = resources.remove(id).map(_.asInstanceOf[T])

  def getResource[T](id: UUID) = resources.get(id).map(_.asInstanceOf[T])

}

/**
  * Scope Companion Object
  * @author lawrence.daniels@gmail.com
  */
object Scope {

  type DynamicValue = () => Any

  def apply() = new Scope()

  def apply(rootScope: Scope) = new Scope(rootScope.toSeq: _*)

}