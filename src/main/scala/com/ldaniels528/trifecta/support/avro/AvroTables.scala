package com.ldaniels528.trifecta.support.avro

import com.ldaniels528.tabular.Tabular
import org.apache.avro.generic.GenericRecord

import scala.collection.JavaConversions._

/**
 * Adds Avro table capabilities to a Tabular instance
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
trait AvroTables {
  self: Tabular =>

  def transformAvro(records: Seq[GenericRecord], reqFields: Seq[String]): Seq[String] = {
    if (records.isEmpty) Seq.empty
    else {
      val fields = if (reqFields.nonEmpty) reqFields else records.head.getSchema.getFields.map(_.name.trim).toSeq
      val rows = records map { r =>
        Map(fields map (k => (k, asString(r.get(k)).trim)): _*)
      }

      makeTable(fields, rows)
    }
  }
}