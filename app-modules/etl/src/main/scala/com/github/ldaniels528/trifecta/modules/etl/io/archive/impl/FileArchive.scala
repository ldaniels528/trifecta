package com.github.ldaniels528.trifecta.modules.etl.io.archive.impl

import com.github.ldaniels528.trifecta.modules.etl.io.archive.CompressionTypes._
import com.github.ldaniels528.trifecta.modules.etl.io.archive.{Archive, CompressionTypes}

/**
  * Represents a file archive
  * @author lawrence.daniels@gmail.com
  */
case class FileArchive(id: String, basePath: String, compressionType: CompressionType = CompressionTypes.NONE) extends Archive