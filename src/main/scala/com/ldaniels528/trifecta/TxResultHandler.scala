package com.ldaniels528.trifecta

import java.io.PrintStream

import com.ldaniels528.tabular.Tabular
import com.ldaniels528.trifecta.support.avro.AvroTables
import com.ldaniels528.trifecta.support.kafka.KafkaMicroConsumer.MessageData
import com.ldaniels528.trifecta.util.BinaryMessaging

import scala.collection.GenTraversableOnce
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

/**
 * Trifecta Result Handler
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class TxResultHandler(config: TxConfig) extends BinaryMessaging {
  // define the tabular instance
  val tabular = new Tabular() with AvroTables
  val out: PrintStream = config.out

  /**
   * Handles the processing and/or display of the given result of a command execution
   * @param result the given result
   * @param ec the given execution context
   */
  def handleResult(result: Any, input: String)(implicit ec: ExecutionContext) {
    result match {
      // handle binary data
      case message: Array[Byte] if message.isEmpty => out.println("No data returned")
      case message: Array[Byte] => dumpMessage(message)(config)
      case MessageData(offset, _, _, _, message) => dumpMessage(offset, message)(config)

      // handle Either cases
      case e: Either[_, _] => e match {
        case Left(v) => handleResult(v, input)
        case Right(v) => handleResult(v, input)
      }

      // handle Future cases
      case f: Future[_] => handleAsyncResult(f, input)

      // handle Option cases
      case o: Option[_] => o match {
        case Some(v) => handleResult(v, input)
        case None => out.println("No data returned")
      }

      // handle Try cases
      case t: Try[_] => t match {
        case Success(v) => handleResult(v, input)
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

  private def handleAsyncResult(task: Future[_], input: String)(implicit ec: ExecutionContext) {
    // capture the start time
    val startTime = System.currentTimeMillis()

    // initially, wait for 5 seconds for the task to complete.
    // if it fails to complete in that time, queue it as an asynchronous job
    Try(Await.result(task, 5.seconds)) match {
      case Success(value) => handleResult(value, input)
      case Failure(e1) =>
        val job = config.jobManager.createJob(startTime, task, input)
        task.onComplete {
          case Success(value) =>
            out.println(s"Job #${job.jobId} completed (use 'jobs -v ${job.jobId}' to view results)")
            handleResult(value, input)
          case Failure(e2) =>
            out.println(s"Job #${job.jobId} failed: ${e2.getMessage}")
        }
        out.println("Task is now running in the background (use 'jobs' to view)")
    }
  }

}
