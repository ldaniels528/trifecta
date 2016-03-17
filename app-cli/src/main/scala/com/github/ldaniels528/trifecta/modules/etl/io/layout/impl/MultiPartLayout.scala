package com.github.ldaniels528.trifecta.modules.etl.io.layout.impl

import com.github.ldaniels528.trifecta.modules.etl.io.Scope
import com.github.ldaniels528.trifecta.modules.etl.io.device._
import com.github.ldaniels528.trifecta.modules.etl.io.layout.Layout
import com.github.ldaniels528.trifecta.modules.etl.io.layout.Layout.InputSet
import com.github.ldaniels528.trifecta.modules.etl.io.layout.impl.MultiPartLayout._
import com.github.ldaniels528.trifecta.modules.etl.io.record.Record

import scala.collection.concurrent.TrieMap

/**
  * Multi-Record Layout implementation
  * @author lawrence.daniels@gmail.com
  */
case class MultiPartLayout(id: String, body: Section, header: Option[Section], trailer: Option[Section]) extends Layout {
  private val buffers = TrieMap[InputSource, List[DataSet]]()

  override def read(device: InputSource)(implicit scope: Scope) = {
    device.getStatistics.offset match {
      // extract-only the optional header record(s)
      case offset if isHeader(offset) =>
        for {
          head <- header
          dataSet <- device.read(head.records(offset.toInt))
        } yield {
          scope ++= dataSet.data
          InputSet(dataSets = Nil, offset = offset, isEOF = false)
        }

      // extract-only the optional trailer record(s)
      case offset if isTrailer(device) =>
        for {
          buffer <- buffers.get(device)
          footer <- trailer
          record = footer.records(footer.records.length - buffer.length)
          dataSet <- device.read(record)
        } yield {
          scope ++= dataSet.data
          InputSet(dataSets = Nil, offset = offset, isEOF = false)
        }

      // extract & populate the body record(s)
      case offset =>
        // TODO implement read ahead here

        val record = body.records(offset.toInt % body.records.length)
        device.read(record) map { dataSet =>
          InputSet(dataSets = Seq(dataSet), offset = offset, isEOF = false)
        }
    }
  }

  private def isHeader(offset: Long) = header.exists(offset < _.records.length)

  private def isTrailer(device: InputSource) = {
    (for {
      buffer <- buffers.get(device)
      footer <- trailer
    } yield buffer.length <= footer.records.length).contains(true)
  }

  override def write(device: OutputSource, inputSet: Option[InputSet])(implicit scope: Scope) {
    inputSet match {
      case Some(is) =>
        // write the header records
        if (device.getStatistics.offset == 0L) {
          header.foreach(_.records foreach { record =>
            device.writeRecord(record, record.toDataSet)
          })
        }

        // write the body
        for {
          record <- body.records
          dataSet <- is.dataSets
        } device.writeRecord(record, dataSet)

      case None =>
        // write the trailer records
        trailer.foreach(_.records foreach { record =>
          device.writeRecord(record, record.toDataSet)
        })
    }
  }

  private def readAhead(record: Record, device: InputSource)(implicit scope: Scope) = {
    var buffer = buffers.getOrElseUpdate(device, Nil)
    var eof = false

    // make sure the head ahead buffer is filled
    var newLines: List[DataSet] = Nil
    while (!eof && buffer.size < readAheadSize) {
      val text = device.read(record)
      eof = text.isEmpty
      text.foreach(item => newLines = item :: newLines)
    }
    buffer = buffer ::: newLines.reverse

    // return the next line from the buffer
    val input = buffer.headOption
    buffer = if (buffer.nonEmpty) buffer.tail else Nil
    buffers(device) = buffer
    input.map(ti => InputData(
      dataSet = ti,
      buffer = buffer,
      isHeader = header.exists(_.records.length >= device.getStatistics.offset),
      isTrailer = trailer.exists(_.records.length > buffer.length)))
  }

  private def readAheadSize: Int = {
    1 + (header.map(_.records.length) getOrElse 0) + (trailer.map(_.records.length) getOrElse 0)
  }

}

/**
  * Multi-Part Layout Companion Object
  * @author lawrence.daniels@gmail.com
  */
object MultiPartLayout {

  /**
    * Document Layout Section
    */
  case class Section(records: Seq[Record])

  case class InputData(dataSet: DataSet, buffer: List[DataSet] = Nil, isHeader: Boolean, isTrailer: Boolean)

}