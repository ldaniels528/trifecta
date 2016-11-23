package com.github.ldaniels528.trifecta.modules.etl.io.archive

/**
  * An enumeration of Compression Types
  * @author lawrence.daniels@gmail.com
  */
object CompressionTypes extends Enumeration {
  type CompressionType = Value

  val NONE, ZIP, GZIP = Value

}
