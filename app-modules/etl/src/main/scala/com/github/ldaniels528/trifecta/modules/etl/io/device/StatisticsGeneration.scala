package com.github.ldaniels528.trifecta.modules.etl.io.device

import java.util.UUID

import com.github.ldaniels528.trifecta.modules.etl.io.Scope
import com.github.ldaniels528.trifecta.modules.etl.io.device.StatisticsGeneration.Statistics

/**
  * I/O Statistic Generation
  * @author lawrence.daniels@gmail.com
  */
trait StatisticsGeneration {
  private val uuid = UUID.randomUUID().toString

  def getStatistics(implicit scope: Scope) = scope.getOrElseUpdate(uuid, Statistics())

  def updateCount(delta: Int)(implicit scope: Scope) = getStatistics.update(delta)

}

/**
  * I/O Statistic Generation Companion Object
  * @author lawrence.daniels@gmail.com
  */
object StatisticsGeneration {

  case class Statistics() {
    var firstUpdate: Long = System.currentTimeMillis()
    var lastUpdate: Long = System.currentTimeMillis()
    var lastRpsUpdate: Long = 0
    var lastCount: Long = 0
    var count: Long = 0
    var offset: Long = 0
    var rps: Double = 0

    def avgRecordsPerSecond = count / (elapsedTimeMillis / 1000d)

    def elapsedTimeMillis = lastUpdate - firstUpdate

    def update(delta: Int) = {
      count += delta
      offset += 1
      lastUpdate = System.currentTimeMillis()

      val diff = (lastUpdate - lastRpsUpdate) / 1000d
      if (lastRpsUpdate == 0 || diff >= 1) {
        if (lastRpsUpdate == 0) firstUpdate = lastUpdate
        rps = if (lastRpsUpdate == 0) count else (count - lastCount) / diff
        lastCount = count
        lastRpsUpdate = lastUpdate
      }
      delta
    }
  }

}
