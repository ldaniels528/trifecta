package com.github.ldaniels528.trifecta.modules.etl.io.trigger.impl

import java.io.File

import com.github.ldaniels528.trifecta.modules.etl.io.archive.Archive
import com.github.ldaniels528.trifecta.modules.etl.io.flow.Flow

/**
  * Represents a File Feed
  * @author lawrence.daniels@gmail.com
  */
case class FileFeed(matches: File => Boolean, flows: Seq[Flow], archive: Option[Archive])

/**
  * File Feed Companion Object
  */
object FileFeed {

  def endsWith(suffix: String, flows: Seq[Flow], archive: Option[Archive]) = {
    FileFeed(matches = _.getName.endsWith(suffix), flows, archive)
  }

  def exact(name: String, flows: Seq[Flow], archive: Option[Archive]) = {
    FileFeed(matches = _.getName == name, flows, archive)
  }

  def ignoreCase(name: String, flows: Seq[Flow], archive: Option[Archive]) = {
    FileFeed(matches = _.getName.equalsIgnoreCase(name), flows, archive)
  }

  def regex(pattern: String, flows: Seq[Flow], archive: Option[Archive]) = {
    FileFeed(matches = _.getName.matches(pattern), flows, archive)
  }

  def startsWith(prefix: String, flows: Seq[Flow], archive: Option[Archive]) = {
    FileFeed(matches = _.getName.startsWith(prefix), flows, archive)
  }

}