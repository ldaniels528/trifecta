package com.github.ldaniels528.trifecta.modules.etl.io.trigger.impl

import java.io.File

import com.github.ldaniels528.trifecta.modules.etl.io.archive.Archive

/**
  * Represents a generic feed container
  * @author lawrence.daniels@gmail.com
  */
trait FeedContainer {

  def archive: Option[Archive]

  def feeds: Seq[FileFeed]

  def find(file: File): Option[FileFeed]

  def path: String

}
