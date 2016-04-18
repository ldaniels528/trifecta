package com.github.ldaniels528.trifecta.modules.etl.io.trigger.impl

import java.io.File

import com.github.ldaniels528.trifecta.modules.etl.io.archive.Archive
import com.github.ldaniels528.trifecta.modules.etl.io.trigger.impl.FileFeedSet.FeedMatch

import scala.language.postfixOps

/**
  * File Feed Set
  * @author lawrence.daniels@gmail.com
  */
case class FileFeedSet(path: String, pattern: String, feeds: Seq[FileFeed], archive: Option[Archive])
  extends FeedContainer {
  private val patternR = pattern.r

  override def find(file: File) = feeds.find(_.matches(file))

  def isSatisfied(files: Seq[File]) = feeds.forall(feed => files.exists(feed.matches))

  def getFiles(files: Seq[File]): Seq[FeedMatch] = {
    if (!isSatisfied(files)) Nil
    else {
      (for {
        feed <- feeds
        file <- files if feed.matches(file)
      } yield {
        val groupId = file.getName match {
          case patternR(v) => Some(v)
          case _ => None
        }
        groupId.map(FeedMatch(_, file, feed))
      }) flatten
    }
  }

}

/**
  * File Feed Set Companion Object
  * @author lawrence.daniels@gmail.com
  */
object FileFeedSet {

  case class FeedMatch(groupId: String, file: File, feed: FileFeed)

}