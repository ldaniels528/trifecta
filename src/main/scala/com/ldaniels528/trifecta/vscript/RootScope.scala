package com.ldaniels528.trifecta.vscript

/**
 * Represents a root execution scope
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class RootScope(parent: Option[Scope]) extends CascadingScope(parent)

/**
 * Root Scope Singleton
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
object RootScope {

  def apply() = new RootScope(None)

  def apply(parent: Scope) = new RootScope(Some(parent))

  def unapply(scope: RootScope) = (scope.parent, scope.getFunctions, scope.getVariables)

}