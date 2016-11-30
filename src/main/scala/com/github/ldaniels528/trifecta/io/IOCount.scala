package com.github.ldaniels528.trifecta.io

/**
  * Represents the results of an I/O operation
  * @param read the number of records read
  * @param written the number of records written
  * @param failures the number of failed records
  * @param recordsPerSecond the transfer rate (in records/second)
  * @param runTimeSecs the complete process run time (in seconds)
  */
case class IOCount(read: Long, written: Long, failures: Long, recordsPerSecond: Double, runTimeSecs: Double)
