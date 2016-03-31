package com.github.ldaniels528.trifecta.modules.etl.io.device.impl

import com.github.ldaniels528.trifecta.modules.etl.io.Scope
import com.github.ldaniels528.trifecta.modules.etl.io.device.{DataSet, OutputSource}
import com.github.ldaniels528.trifecta.modules.etl.io.layout.Layout
import com.github.ldaniels528.trifecta.modules.etl.io.record._
import org.slf4j.LoggerFactory

/**
  * Loop-back OutputSource
  * @author lawrence.daniels@gmail.com
  */
case class LoopBackOutputSource(id: String, layout: Layout) extends OutputSource {
  private val logger = LoggerFactory.getLogger(getClass)
  private var offset = 0L

  override def writeRecord(record: Record, dataSet: DataSet)(implicit scope: Scope) = {
    val text: String = record match {
      case rec: BinarySupport => new String(rec.toBytes(dataSet))
      case rec: TextSupport => rec.toText(dataSet)
      case rec: JsonSupport => rec.toJson(dataSet).toString()
      case rec => s"[${rec.asString}]"
    }
    logger.info(f"[$offset%06d] $text")
    offset += 1
    1
  }

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
