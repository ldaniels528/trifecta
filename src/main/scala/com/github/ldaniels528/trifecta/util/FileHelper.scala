package com.github.ldaniels528.trifecta.util

import java.io.File

import scala.io.Source

/**
  * File Helper
  * @author lawrence.daniels@gmail.com
  */
object FileHelper {

  implicit class FileEnrichment(val file: File) extends AnyVal {

    def getTextContents: String = Source.fromFile(file).getLines().mkString("\n")

    def extension: Option[String] = {
      val name = file.getName
      name.lastIndexOf('.') match {
        case -1 => None
        case index => Some(name.substring(index).toLowerCase)
      }
    }

  }

}
