package com.ldaniels528.verify.io.avro

import com.ldaniels528.tabular.Tabular

/**
 * Adds Avro table capabilities to a Tabular instance
 * @author lawrence.daniels@gmail.com
 */
trait AvroTables { self : Tabular =>
  import scala.collection.JavaConversions._
  import org.apache.avro.generic.GenericRecord

  def transformAvro(records: Seq[GenericRecord], reqFields: Seq[String]): Seq[String] = {
    if (records.isEmpty) Seq.empty
    else {
      val fields = if (reqFields.nonEmpty) reqFields else records.head.getSchema().getFields().map(_.name.trim).toSeq.slice(0, 20)
      val rows = records map { r =>
        Map(fields map (k => (k, asString(r.get(k)).trim)): _*)
      }

      makeTable(fields, rows)
    }
  }
}