package com.ldaniels528.verify.vscript

import com.ldaniels528.verify.vscript.VScriptRuntime.ConstantValue
import scala.collection.concurrent.TrieMap

/**
 * Represents a cascading scope
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class CascadingScope(val parent: Option[Scope]) extends Scope {
  private val classes = TrieMap[String, ClassDef]()
  private val functions = TrieMap[String, Function]()
  private val variables = TrieMap[String, Variable]()

  override def toString = {
    val sb = new StringBuilder()
    sb ++= s"variables = ${getVariables.toList}, functions = ${getFunctions.toList}"
    parent match {
      case Some(scope) => sb ++= s", { $scope }"
      case None =>
    }
    sb.toString()
  }

  /////////////////////////////////////////////////////////////////
  //      Classes
  /////////////////////////////////////////////////////////////////

  override def +=(c: ClassDef) = {
    classes += (c.name -> c)
    this
  }

  override def getClassDef(name: String): Option[ClassDef] = {
    classes.get(name) match {
      case Some(c) => Some(c)
      case None => parent flatMap (_.getClassDef(name))
    }
  }

  /**
   * Retrieves all classes from the scope
   */
  override def getClassDefs: Seq[ClassDef] = {
    classes.values.toSeq ++ (parent map (_.getClassDefs) getOrElse Seq.empty)
  }

  /////////////////////////////////////////////////////////////////
  //      Functions
  /////////////////////////////////////////////////////////////////

  override def +=(f: Function) = {
    functions += (f.name -> f)
    this
  }

  override def getFunction(name: String): Option[Function] = {
    functions.get(name) match {
      case Some(f) => Some(f)
      case None => parent flatMap (_.getFunction(name))
    }
  }

  override def getFunctions: Seq[Function] = {
    functions.values.toSeq ++ (parent map (_.getFunctions) getOrElse Seq.empty)
  }

  /////////////////////////////////////////////////////////////////
  //      Variables
  /////////////////////////////////////////////////////////////////

  override def +=(v: Variable) = {
    variables += (v.name -> v)
    this
  }

  override def getValue[T](name: String)(implicit scope: Scope): Option[T] = {
    getVariable(name) flatMap (_.eval) map (_.asInstanceOf[T])
  }

  override def setValue(name: String, value: OpCode) {
    getVariable(name) foreach (_.value = value)
  }

  override def setValue[T](name: String, value: Option[T]) {
    getVariable(name) foreach (_.value = ConstantValue(value))
  }

  override def getVariable(name: String): Option[Variable] = {
    variables.get(name) match {
      case Some(v) => Some(v)
      case None => parent flatMap (_.getVariable(name))
    }
  }

  override def getVariables: Seq[Variable] = {
    variables.values.toSeq ++ (parent map (_.getVariables) getOrElse Seq.empty)
  }

  /////////////////////////////////////////////////////////////////
  //      Named Entities
  /////////////////////////////////////////////////////////////////

  /**
   * Searches the scope hierarchy for an entity matching the given name
   */
  override def getNamedEntity(name: String): Option[NamedEntity] = {
    Option(getVariable(name) getOrElse (getFunction(name) getOrElse getClassDef(name).orNull))
  }

  /**
   * Searches the scope hierarchy for an entity matching the given name
   */
  override def getNamedEntities: Seq[NamedEntity] = getClassDefs ++ getFunctions ++ getVariables

}

/**
 * Cascading Scope Singleton
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
object CascadingScope {

  def unapply(scope: CascadingScope) = (scope.parent, scope.getFunctions, scope.getVariables)

}
