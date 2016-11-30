package com.github.ldaniels528.trifecta.io

import scala.concurrent.{ExecutionContext, Future}

/**
 * This class acts as a wrapper for an asynchronous job processing
 * an input, an output both.
 * @author lawrence.daniels@gmail.com
 */
case class AsyncIO(task: Future[_], counter: IOCounter) {
  val startTime: Long = counter.startTimeMillis
  val endTime: Long = counter.lastUpdateMillis

  def getCount: Seq[IOCount] = Seq(counter.get)

}

/**
 * Asynchronous I/O
 * @author lawrence.daniels@gmail.com
 */
object AsyncIO {

  /**
   * Syntactic sugar for executing asynchronous I/O as an executable block
   * @param block the given code block
   * @return the asynchronous I/O instance
   */
  def apply(block: IOCounter => Unit)(implicit ec: ExecutionContext): AsyncIO = {
    val counter = IOCounter(System.currentTimeMillis())
    AsyncIO(Future(block), counter)
  }

}
