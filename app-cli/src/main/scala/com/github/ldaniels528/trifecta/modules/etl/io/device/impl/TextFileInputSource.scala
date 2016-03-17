package com.github.ldaniels528.trifecta.modules.etl.io.device.impl

import java.io.{BufferedReader, File, FileReader}
import java.util.UUID

import com.github.ldaniels528.trifecta.modules.etl.io.Scope
import com.github.ldaniels528.trifecta.modules.etl.io.device.InputSource
import com.github.ldaniels528.trifecta.modules.etl.io.layout._
import com.github.ldaniels528.trifecta.modules.etl.io.record.{Record, TextSupport, UnsupportedRecordTypeException}
import org.slf4j.LoggerFactory

/**
  * Text File Input Source
  * @author lawrence.daniels@gmail.com
  */
case class TextFileInputSource(id: String, path: String, layout: Layout) extends InputSource {
  private val log = LoggerFactory.getLogger(getClass)
  private val uuid = UUID.randomUUID()

  override def close(implicit scope: Scope) = {
    scope.discardResource[BufferedReader](uuid).foreach(_.close())
  }

  override def open(implicit scope: Scope) = {
    val file = new File(scope.evaluateAsString(path))
    scope ++= Seq(
      "flow.input.id" -> id,
      "flow.input.count" -> (() => getStatistics(scope).count),
      "flow.input.filename" -> file.getName,
      "flow.input.lastModified" -> (() => file.lastModified()),
      "flow.input.length" -> (() => file.length()),
      "flow.input.offset" -> (() => getStatistics(scope).offset),
      "flow.input.path" -> file.getCanonicalPath
    )
    log.info(s"Opening input file '${file.getAbsolutePath}'...")
    scope.createResource(uuid, new BufferedReader(new FileReader(file)))
    ()
  }

  override def read(record: Record)(implicit scope: Scope) = {
    for {
      reader <- scope.getResource[BufferedReader](uuid)
      line <- Option(reader.readLine)
      _ = updateCount(1)
      offset = getStatistics.offset
    } yield {
      record match {
        case rec: TextSupport => rec.fromText(line)
        case rec => throw new UnsupportedRecordTypeException(rec)
      }
    }
  }

}

