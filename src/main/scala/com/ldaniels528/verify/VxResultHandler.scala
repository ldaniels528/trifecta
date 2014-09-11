package com.ldaniels528.verify

import java.io.PrintStream

import com.ldaniels528.tabular.Tabular
import com.ldaniels528.verify.VxRuntimeContext.JobItem
import com.ldaniels528.verify.modules.avro.AvroTables
import com.ldaniels528.verify.support.kafka.KafkaSubscriber
import KafkaSubscriber.MessageData
import com.ldaniels528.verify.util.BinaryMessaging

import scala.collection.GenTraversableOnce
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

/**
 * Verify Result Handler
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class VxResultHandler(rt: VxRuntimeContext) extends BinaryMessaging {
  // define the tabular instance
  val tabular = new Tabular() with AvroTables
  val out: PrintStream = rt.out

  /**
   * Handles the processing and/or display of the given result of a command execution
   * @param result the given result
   * @param ec the given execution context
   */
  def handleResult(result: Any)(implicit ec: ExecutionContext) {
    result match {
      // handle binary data
      case message: Array[Byte] if message.isEmpty => out.println("No data returned")
      case message: Array[Byte] => dumpMessage(message)(rt, out)
      case MessageData(offset, _, _, message) => dumpMessage(offset, message)(rt, out)

      // handle Either cases
      case e: Either[_, _] => e match {
        case Left(v) => handleResult(v)
        case Right(v) => handleResult(v)
      }

      // handle Future cases
      case f: Future[_] => handleAsyncResult(f)

      // handle Option cases
      case o: Option[_] => o match {
        case Some(v) => handleResult(v)
        case None => out.println("No data returned")
      }

      // handle Try cases
      case t: Try[_] => t match {
        case Success(v) => handleResult(v)
        case Failure(e) => throw e
      }

      // handle lists and sequences of case classes
      case s: Seq[_] if s.isEmpty => out.println("No data returned")
      case s: Seq[_] if !Tabular.isPrimitives(s) => tabular.transform(s) foreach out.println

      // handle lists and sequences of primitives
      case g: GenTraversableOnce[_] => g foreach out.println

      // anything else ...
      case x => if (x != null && !x.isInstanceOf[Unit]) out.println(x)
    }
  }

  private def handleAsyncResult(f: Future[_])(implicit ec: ExecutionContext) {
    // initially, wait for 3 seconds for the task to complete.
    // if it fails to complete in that time, queue it as an asynchronous job
    Try(Await.result(f, 3.seconds)) match {
      case Success(value) => handleResult(value)
      case Failure(e) => setupAsyncJob(f)
    }
  }

  private def setupAsyncJob(f: Future[_])(implicit ec: ExecutionContext): Unit = {
    val job = JobItem(startTime = System.currentTimeMillis(), task = f)
    rt.jobs += job
    f.onComplete {
      case Success(value) =>
        out.println(s"Job #${job.jobId} completed")
        rt.jobs -= job
        handleResult(value)
      case Failure(e) =>
        out.println(s"Job #${job.jobId} failed: ${e.getMessage}")
        rt.jobs -= job
    }
    handleResult(Seq(job))
  }

}
