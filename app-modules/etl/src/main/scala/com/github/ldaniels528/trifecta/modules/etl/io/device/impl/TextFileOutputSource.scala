package com.github.ldaniels528.trifecta.modules.etl.io.device.impl

import java.io.{BufferedWriter, File, FileWriter}
import java.util.UUID

import com.github.ldaniels528.trifecta.modules.etl.io.Scope
import com.github.ldaniels528.trifecta.modules.etl.io.device.{DataSet, OutputSource}
import com.github.ldaniels528.trifecta.modules.etl.io.layout._
import com.github.ldaniels528.trifecta.modules.etl.io.record.Record
import com.github.ldaniels528.commons.helpers.OptionHelper._
import org.slf4j.LoggerFactory

/**
  * Text File Output Source
  * @author lawrence.daniels@gmail.com
  */
case class TextFileOutputSource(id: String, path: String, layout: Layout) extends OutputSource {
  private val log = LoggerFactory.getLogger(getClass)
  private val uuid = UUID.randomUUID()

  override def close(implicit scope: Scope) = {
    scope.discardResource[BufferedWriter](uuid).foreach(_.close())
  }

  override def open(implicit scope: Scope) = {
    val file = new File(scope.evaluateAsString(path))
    scope ++= Seq(
      "flow.output.id" -> id,
      "flow.output.count" -> (() => getStatistics.count),
      "flow.output.filename" -> file.getName,
      "flow.output.lastModified" -> (() => file.lastModified()),
      "flow.output.length" -> (() => file.length()),
      "flow.output.offset" -> (() => getStatistics.offset),
      "flow.output.path" -> file.getCanonicalPath
    )
    log.info(s"Opening output file '${file.getAbsolutePath}'...")
    scope.createResource(uuid, new BufferedWriter(new FileWriter(file)))
    ()
  }

  override def writeRecord(record: Record, dataSet: DataSet)(implicit scope: Scope) = {
    scope.getResource[BufferedWriter](uuid) map { writer =>
      writer.write(dataSet.convertToText(record))
      writer.newLine()
      updateCount(1)
    } orDie s"Text file output source '$id' has not been opened"
  }

}
