package com.github.ldaniels528.trifecta

import java.io.File

/**
  * Command Line Helper
  * @author lawrence.daniels@gmail.com
  */
object CommandLineHelper {

  implicit class ArgumentEnrichment(val args: Array[String]) extends AnyVal {

    @inline
    def filterShellArgs: Array[String] = args.filterNot(_.startsWith("--"))

    @inline
    def isNonInteractive: Boolean = args.exists(!_.startsWith("--"))

    @inline
    def isDebug: Boolean = args.contains("--debug")

    @inline
    def isPrettyJson: Boolean = args.contains("--pretty-json")

    @inline
    def isKafkaSandbox: Boolean = args.contains("--kafka-sandbox")

    def scriptFile(args: Seq[String]): Option[File] = {
      args.indexOf("-script-file") match {
        case index if index >= 0 && index + 1 < args.length => Some(new File(args(index + 1)))
        case _ => None
      }
    }

  }

}
