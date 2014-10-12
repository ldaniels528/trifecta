package com.ldaniels528.trifecta

import java.util.concurrent.atomic.AtomicLong

import com.google.common.util.concurrent.AtomicDouble
import com.ldaniels528.trifecta.modules.Module.IOCount

import scala.concurrent.Future

/**
 * This class acts as a wrapper for an asynchronous job processing
 * an input, an output both.
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
case class AsyncIO(startTime: Long, task: Future[_], read: AtomicLong, written: AtomicLong, rps: AtomicDouble) {
  val endTime = new AtomicLong(0L)

  def getCount: Seq[IOCount] = {
    val runTimeSecs = Math.round(10d * ((System.currentTimeMillis() - startTime).toDouble / 1000d)) / 10d
    Seq(IOCount(read.get, written.get, failures = 0, rps.get, runTimeSecs))
  }

}
