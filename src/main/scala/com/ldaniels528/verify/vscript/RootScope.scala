package com.ldaniels528.verify.vscript

/**
 * Represents a root execution scope
 * @author lawrence.daniels@gmail.com
 */
class RootScope(parent: Option[Scope]) extends CascadingScope(parent)

/**
 * Root Scope Singleton
 * @author lawrence.daniels@gmail.com
 */
object RootScope {

  def apply() = new RootScope(None)

  def apply(parent: Scope) = new RootScope(Some(parent))

  def unapply(scope: RootScope) = (scope.parent, scope.getFunctions, scope.getVariables)

}