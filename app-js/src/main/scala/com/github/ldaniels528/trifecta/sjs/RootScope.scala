package com.github.ldaniels528.trifecta.sjs

import io.scalajs.npm.angularjs.Scope

import scala.scalajs.js

/**
  * Trifecta Application Root Scope
  * @author lawrence.daniels@gmail.com
  */
@js.native
trait RootScope extends Scope {
  var version: js.UndefOr[String] = js.native
  var kafkaVersion: js.UndefOr[String] = js.native
  var zookeeper: js.UndefOr[String] = js.native
}
