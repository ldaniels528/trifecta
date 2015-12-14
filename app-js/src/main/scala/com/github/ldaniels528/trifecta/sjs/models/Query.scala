package com.github.ldaniels528.trifecta.sjs.models

import com.github.ldaniels528.scalascript.util.ScalaJsHelper._

import scala.scalajs.js

/**
  * Represents a Query Model
  * @author lawrence.daniels@gmail.com
  */
@js.native
trait Query extends js.Object {
  var name: js.UndefOr[String] = js.native
  var topic: js.UndefOr[String] = js.native
  var modified: js.UndefOr[Boolean] = js.native
  var queryString: js.UndefOr[String] = js.native
  var collection: js.UndefOr[TopicDetails] = js.native
  var results: js.UndefOr[js.Array[QueryRow]] = js.native
  var savedResult: js.UndefOr[SavedResult] = js.native

  // ui-specific properties
  var newFile: js.UndefOr[Boolean] = js.native
  var queryElaspedTime: js.UndefOr[Int] = js.native
  var queryStartTime: js.UndefOr[Int] = js.native
  var running: js.UndefOr[Boolean] = js.native
  var syncing: js.UndefOr[Boolean] = js.native
}

/**
  * Query Companion Object
  * @author lawrence.daniels@gmail.com
  */
object Query {

  def apply(name: js.UndefOr[String],
            topic: js.UndefOr[String],
            newFile: js.UndefOr[Boolean] = true,
            modified: js.UndefOr[Boolean] = true) = {
    val query = makeNew[Query]
    query.name = name
    query.topic = topic
    query.newFile = newFile
    query.modified = modified
    query
  }

}