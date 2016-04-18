package com.github.ldaniels528.trifecta.io.avro

import com.github.ldaniels528.tabular.Tabular
import org.apache.avro.generic.GenericRecord

import scala.collection.JavaConversions._

/**
 * Adds Avro table capabilities to a Tabular instance
 * @author lawrence.daniels@gmail.com
 */
trait AvroTables {
  self: Tabular =>

  def transformAvro(records: Seq[GenericRecord], reqFields: Seq[String]): Seq[String] = {
    if (records.isEmpty) Nil
    else {
      val fields = if (reqFields.nonEmpty) reqFields else records.head.getSchema.getFields.map(_.name.trim).toSeq
      val rows = records map { r =>
        Map(fields map (k => (k, asString(r.get(k)).trim)): _*)
      }

      makeTable(fields, rows)
    }
  }
}