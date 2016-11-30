package com.github.ldaniels528.trifecta.io

import java.util.concurrent.atomic.AtomicLong

import com.google.common.util.concurrent.AtomicDouble

/**
  * I/O Counter
  * @param startTimeMillis the given start time of the process being tracked
  */
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
