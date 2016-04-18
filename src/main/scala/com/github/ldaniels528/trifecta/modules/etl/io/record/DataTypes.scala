package com.github.ldaniels528.trifecta.modules.etl.io.record

/**
  * Data Types Enumeration
  * @author lawrence.daniels@gmail.com
  */
object DataTypes extends Enumeration {
  type DataType = Value

  val BINARY, BOOLEAN, DATE, DOUBLE, FLOAT, INT, LONG, STRING = Value

  /**
    * Data Type Enrichment
    *
    * @param dataType the given [[DataType data type]]
    */
  implicit class DataTypeEnrichment(val dataType: DataType) extends AnyVal {

    def toTypeName = dataType.toString.toLowerCase
  }

}
