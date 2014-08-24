package com.ldaniels528.verify

import org.fusesource.jansi.Ansi._
import org.fusesource.jansi.{Ansi, AnsiConsole}
import org.slf4j.LoggerFactory

/**
 * Verify Console
 */
object VxConsole {
  private lazy val logger = LoggerFactory.getLogger(getClass)
  val out = System.out

  def install(): Unit = AnsiConsole.systemInstall()

  def uninstall(): Unit = AnsiConsole.systemUninstall()

  def wrap[T](block: => T): T = {
    install()
    val result = block
    uninstall()
    result
  }

  /**
   * ANSI String Interpolation
   * @param sc the given string context
   */
  implicit class AnsiInterpolation(val sc: StringContext) extends AnyVal {

    def a(args: Any*): Ansi = {
      val items = sc.parts.iterator
      val expressions = args.iterator

      val result = (items zip expressions).foldLeft[Ansi](ansi()) { case (results, (s, expr)) =>
        logger.info(s"a: s = $s, expr = $expr")
        results
      }
      result.reset()
    }
  }

}
