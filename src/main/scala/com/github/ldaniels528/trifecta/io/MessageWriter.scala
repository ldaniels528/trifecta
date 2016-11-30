package com.github.ldaniels528.trifecta.io

/**
  * Message Writer
  * @author lawrence.daniels@gmail.com
  */
trait MessageWriter {

  /**
    * Attempts to retrieve an output source for the given URL
    * @param url the given output URL
    * @return the option of an [[MessageOutputSource output source]]
    */
  def getOutputSource(url: String): Option[MessageOutputSource]

}
