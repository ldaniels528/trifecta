package com.github.ldaniels528.trifecta.modules.etl.io.device.impl

import java.util.concurrent.Callable

import akka.util.Timeout
import com.github.ldaniels528.trifecta.modules.etl.actors.{BroadwayActorSystem, TaskActorPool}
import com.github.ldaniels528.trifecta.modules.etl.io.Scope
import com.github.ldaniels528.trifecta.modules.etl.io.device.impl.ConcurrentOutputSource._
import com.github.ldaniels528.trifecta.modules.etl.io.device.{AsynchronousOutputSupport, DataSet, OutputSource}
import com.github.ldaniels528.trifecta.modules.etl.io.record.Record
import com.github.ldaniels528.commons.helpers.OptionHelper._

import scala.concurrent.ExecutionContext
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.language.implicitConversions

/**
  * Concurrent Output Source (Asynchronous implementation)
  * @author lawrence.daniels@gmail.com
  */
case class ConcurrentOutputSource(id: String, concurrency: Int, devices: Seq[OutputSource])
  extends OutputSource with AsynchronousOutputSupport {

  private val taskActorPool = new TaskActorPool(concurrency)
  private var ticker = 0
  private var lastWrite = 0L

  val layout = devices.headOption.map(_.layout) orDie "No output devices configured"

  override def allWritesCompleted(implicit scope: Scope, ec: ExecutionContext) = {
    taskActorPool.die(4.hours) map (_ => devices)
  }

  override def close(implicit scope: Scope) = delayedClose

  override def open(implicit scope: Scope) = {
    scope ++= Seq(
      "flow.output.id" -> id,
      "flow.output.concurrency" -> concurrency,
      "flow.output.devices" -> devices.length
    )
    devices.foreach(_.open(scope))
  }

  override def writeRecord(record: Record, dataSet: DataSet)(implicit scope: Scope) = {
    implicit val timeout: Timeout = 30.seconds
    lastWrite = System.currentTimeMillis()
    ticker += 1
    val promise = taskActorPool ? (() => devices(ticker % devices.length).writeRecord(record, dataSet))
    promise foreach updateCount
    0
  }

  private def delayedClose(implicit scope: Scope) {
    BroadwayActorSystem.scheduler.scheduleOnce(delay = delayedCloseTime) {
      if (System.currentTimeMillis() - lastWrite >= delayedCloseTime.toMillis) devices.foreach(_.close) else delayedClose
    }
    ()
  }

}

/**
  * Concurrent Output Source Companion Object
  */
object ConcurrentOutputSource {
  private val delayedCloseTime = 10.minutes

  /**
    * Syntactic sugar for creating [[Callable callable]] implementations from anonymous functions
    * @param f the given anonymous function
    * @tparam T the generic return type
    * @return a typed callable instance
    */
  implicit def function2Callable[T](f: () => T): Callable[T] = new Callable[T] {
    override def call = f()
  }

}