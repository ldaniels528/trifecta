package com.github.ldaniels528.trifecta.util

import org.slf4j.LoggerFactory

import scala.util.{Failure, Success, Try}

/**
  * Try Helper
  * @author lawrence.daniels@gmail.com
  */
object TryHelper {
  private val logger = LoggerFactory.getLogger(getClass)

  def sequence[R](seq: Seq[Try[R]]): Try[Seq[R]] = {
    ///seq.zipWithIndex foreach { case (v, n) => logger.info(f"[$n%02d/${seq.length}%02d] $v") }
    Try(seq.foldLeft[List[R]](Nil) {
      case (list, Success(v)) => v :: list
      case (list, Failure(e)) =>
        logger.error(e.getMessage)
        list
    })
  }

}
