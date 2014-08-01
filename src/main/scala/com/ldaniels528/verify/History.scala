package com.ldaniels528.verify

/**
 * Command History Container
 * @author ldaniels
 */
class History(val maxHistory: Int) {
  private var history: List[String] = Nil

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
      history = history.tail
    }
  }

  /**
   * Returns all previously issued commands for this session
   * @return the [[Seq]]uence of [[String]]s representing the previously issued commands for this session
   */
  def getLines: Seq[String] = history

}