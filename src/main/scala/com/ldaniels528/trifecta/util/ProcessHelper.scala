package com.ldaniels528.trifecta.util

import java.io.{ByteArrayOutputStream, PrintStream}

import scala.concurrent.duration.FiniteDuration
import scala.language._

/**
 * Process Helper Utility Class
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
object ProcessHelper {

  /**
   * Executes a block; capturing any generated output to STDOUT and/or STDERR
   * @param initialSize the initial size of the buffer
   * @param block the code block to execute
   * @tparam S the return type of the code block
   * @return byte arrays representing STDOUT and STDERR and the return value of the block
   */
  def sandbox[S](initialSize: Int = 256)(block: => S): (Array[Byte], Array[Byte], S) = {
    // capture standard output & error and create my own buffers
    val out = System.out
    val err = System.err
    val outBuf = new ByteArrayOutputStream(initialSize)
    val errBuf = new ByteArrayOutputStream(initialSize)

    // redirect standard output and error to my own buffers
    System.setOut(new PrintStream(outBuf))
    System.setErr(new PrintStream(errBuf))

    // execute the block; capturing standard output & error
    val result = try block finally {
      System.setOut(out)
      System.setErr(err)
    }
    (outBuf.toByteArray, errBuf.toByteArray, result)
  }

}
