package com.github.ldaniels528.trifecta

import java.io.File

import com.github.ldaniels528.commons.helpers.ResourceHelper._

import scala.util.Try

/**
 * Command History Container
 * @author lawrence.daniels@gmail.com
 */
class History(val maxHistory: Int) {
  private var history: List[String] = Nil
  protected[trifecta] var isDirty: Boolean = _

  /**
   * Returns the previously issued command for the given index
   * @param index the given index
   * @return an [[Option]] of a [[String]] that represents the issued command
   */
  def apply(index: Int): Option[String] = {
    if (index >= 0 && index < history.size) Some(history(index)) else None
  }

  /**
   * Adds a previously issued command to the collection
   * @param line the given previously issued command
   */
  def +=(line: String) {
    history = line :: history
    if (history.size > maxHistory) {
      history = history.init
    }
    isDirty = true
  }

  /**
   * Returns all previously issued commands for this session
   * @return the [[Seq]]uence of [[String]]s representing the previously issued commands for this session
   */
  def getLines(count: Int = -1): Seq[String] = if (count > 0) history.slice(0, count) else history

  def isEmpty: Boolean = history.isEmpty

  def last: Option[String] = history.headOption

  /**
   * Loads history from a the given file
   */
  def load(file: File): Try[Int] = {
    import scala.io.Source

    // ensure that the parent directory exists
    ensureParentDirectory(file)

    Try {
      val lines = Source.fromFile(file).getLines().toSeq.reverse
      lines foreach (line => history = line :: history)
      lines.size
    }
  }

  def nonEmpty: Boolean = history.nonEmpty

  /**
   * Stores the history as the given file
   */
  def store(file: File) {
    import java.io._

    // ensure that the parent directory exists
    ensureParentDirectory(file)

    new BufferedWriter(new FileWriter(file)) use { out =>
      history foreach { line =>
        out.write(line)
        out.newLine()
      }
    }
  }

  def size = history.size

  /**
   * Ensures that the directory, containing the given file, exists
   * @param file the given [[File]]
   */
  private def ensureParentDirectory(file: File) {
    // does the parent directory exist?
    val parentFile = file.getParentFile
    if (!parentFile.exists) {
      parentFile.mkdirs()
    }
    ()
  }

}