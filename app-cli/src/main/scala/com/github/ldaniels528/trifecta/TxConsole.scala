package com.github.ldaniels528.trifecta

import org.fusesource.jansi.Ansi._
import org.fusesource.jansi.{Ansi, AnsiConsole}

/**
 * Trifecta ANSI Console
 * @author lawrence.daniels@gmail.com
 */
object TxConsole {

  def vxAnsi(block: => Unit): Unit = {
    try {
      AnsiConsole.systemInstall()
      block
    }
    finally {
      AnsiConsole.systemUninstall()
    }
  }

  /**
   * ANSI String Interpolation
   * @param sc the given string context
   */
  implicit class AnsiInterpolation(val sc: StringContext) extends AnyVal {

    def a(args: Any*): Ansi = {
      // generate the ANSI string
      getParameterList(args).foldLeft[Ansi](ansi()) { case (results, arg) =>
        arg match {
          case color: Color => results.fg(color)
          case elem => results.a(elem)
        }
      }.reset()
    }

    private def getParameterList(args: Seq[Any]): List[Any] = {
      val (textIt, exprIt) = (sc.parts.iterator, args.toIterator)
      var params: List[Any] = Nil
      while (textIt.hasNext || exprIt.hasNext) {
        if (textIt.hasNext) params = textIt.next() :: params
        if (exprIt.hasNext) params = exprIt.next() :: params
      }
      params.reverse
    }
  }

}
