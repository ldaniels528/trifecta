package com.github.ldaniels528.trifecta.modules.etl.io.device.impl

import com.github.ldaniels528.trifecta.modules.etl.io.Scope
import com.github.ldaniels528.trifecta.modules.etl.io.device.{DataSet, OutputSource}
import com.github.ldaniels528.trifecta.modules.etl.io.layout.Layout
import com.github.ldaniels528.trifecta.modules.etl.io.record.Record

/**
  * Loop-back OutputSource
  * @author lawrence.daniels@gmail.com
  */
case class LoopBackOutputSource(id: String, layout: Layout) extends OutputSource {

  override def writeRecord(record: Record, dataSet: DataSet)(implicit scope: Scope) = 1

  override def close(implicit scope: Scope) = ()

  override def open(implicit scope: Scope) = {
    scope ++= Seq(
      "flow.output.id" -> id,
      "flow.output.count" -> (() => getStatistics.count),
      "flow.output.offset" -> (() => getStatistics.offset)
    )
    ()
  }

}
