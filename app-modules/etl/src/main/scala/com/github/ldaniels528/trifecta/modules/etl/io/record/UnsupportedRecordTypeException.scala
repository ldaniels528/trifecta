package com.github.ldaniels528.trifecta.modules.etl.io.record

/**
  * Unsupported Record Type Exception
  */
class UnsupportedRecordTypeException(record: Record)
  extends RuntimeException(s"Unsupported record type '${record.getClass.getSimpleName}'")