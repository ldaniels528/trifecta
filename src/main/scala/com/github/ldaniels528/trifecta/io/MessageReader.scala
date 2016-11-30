package com.github.ldaniels528.trifecta.io

/**
  * Message Reader
  * @author lawrence.daniels@gmail.com
  */
trait MessageReader {

  /**
    * Attempts to retrieve an input source for the given URL
    * @param url the given input URL
    * @return the option of an [[MessageInputSource input source]]
    */
  def getInputSource(url: String): Option[MessageInputSource]

}
