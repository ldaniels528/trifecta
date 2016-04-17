package com.github.ldaniels528.trifecta.io

import java.util.concurrent.atomic.AtomicLong

import com.google.common.util.concurrent.AtomicDouble
import com.github.ldaniels528.trifecta.io.AsyncIO.{IOCount, IOCounter}

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

  case class IOCounter(startTimeMillis: Long) {
    private var lastCount: Long = 0L

    val read = new AtomicLong(0L)
    val written = new AtomicLong(0L)
    val failed = new AtomicLong(0L)
    val rate = new AtomicDouble(0d)
    var lastUpdateMillis: Long = startTimeMillis

    def get: IOCount = IOCount(read.get, written.get, failed.get, rate.get, runTimeInSeconds)

    def runTimeInSeconds: Double = round((lastUpdateMillis - startTimeMillis).toDouble / 1000d)

    def updateReadCount(delta: Long): Long = update(read.addAndGet(delta))

    def updateWriteCount(delta: Long): Long = written.addAndGet(delta)

    private def update(records: Long): Long = {
      val elapsedTime = (System.currentTimeMillis() - lastUpdateMillis).toDouble / 1000d
      if (elapsedTime >= 1) {
        rate.set(round((records - lastCount).toDouble / elapsedTime))
        lastCount = records
        lastUpdateMillis = System.currentTimeMillis()
      }
      records
    }

    private def round(value: Double): Double = Math.round(10d * value) / 10d

  }

  /**
   * Represents the results of an I/O operation
   * @param read the number of records read
   * @param written the number of records written
   * @param failures the number of failed records
   * @param recordsPerSecond the transfer rate (in records/second)
   * @param runTimeSecs the complete process run time (in seconds)
   */
  case class IOCount(read: Long, written: Long, failures: Long, recordsPerSecond: Double, runTimeSecs: Double)

}
