package com.github.ldaniels528.trifecta.modules.etl.io.archive

/**
  * Represents an archive; a generic data storage system
  * @author lawrence.daniels@gmail.com
  */
trait Archive {

  def id: String

  def basePath: String

}

