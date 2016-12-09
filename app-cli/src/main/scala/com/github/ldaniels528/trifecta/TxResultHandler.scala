package com.github.ldaniels528.trifecta

import java.io.PrintStream

import com.github.ldaniels528.tabular.Tabular
import com.github.ldaniels528.trifecta.io.AsyncIO
import com.github.ldaniels528.trifecta.io.kafka.KafkaMicroConsumer.MessageData
import com.github.ldaniels528.trifecta.io.kafka.StreamedMessage
import com.github.ldaniels528.trifecta.messages.BinaryMessaging
import com.github.ldaniels528.trifecta.messages.codec.avro.AvroTables
import com.github.ldaniels528.trifecta.messages.codec.json.JsonHelper
import com.github.ldaniels528.trifecta.messages.query.KQLResult
import net.liftweb.json.JValue
import org.apache.avro.generic.GenericRecord
import play.api.libs.json.{JsValue, Json}

import scala.collection.GenTraversableOnce
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

/**
  * Trifecta Result Handler
  * @author lawrence.daniels@gmail.com
  */
class TxResultHandler(config: TxConfig, jobManager: JobManager, nonInteractiveMode: Boolean, prettyJson: Boolean) extends BinaryMessaging {
  // define the tabular instance
  val tabular = new Tabular() with AvroTables
  val out: PrintStream = config.out

  /**
    * Handles the processing and/or display of the given result of a command execution
    * @param result the given result
    * @param ec     the given execution context
    */
  def handleResult(result: Any, input: String)(implicit ec: ExecutionContext): Unit = {
    result match {
      // handle the asynchronous I/O cases
      case a: AsyncIO => handleAsyncResult(a, input)

      // handle binary data
      case message: Array[Byte] if message.isEmpty => out.println("No data returned")
      case message: Array[Byte] => dumpMessage(message)(config)
      case MessageData(_, offset, _, _, _, message) => dumpMessage(offset, message)(config)
      case StreamedMessage(_, _, offset, _, message) => dumpMessage(offset, message)(config)

      // handle Either cases
      case e: Either[_, _] => e match {
        case Left(v) => handleResult(v, input)
        case Right(v) => handleResult(v, input)
      }

      // handle Future cases
      case f: Future[_] => handleAsyncResult(f, input)

      // handle Avro records
      case g: GenericRecord =>
        Try(JsonHelper.transform(g)) match {
          case Success(js) => out.println(JsonHelper.renderJson(js, pretty = isPretty))
          case Failure(e) => out.println(g)
        }

      // handle JSON values
      case js: JValue => out.println(JsonHelper.renderJson(js, pretty = isPretty))
      case js: JsValue => out.println(Json.prettyPrint(js))

      // handle Option cases
      case o: Option[_] => o match {
        case Some(v) => handleResult(v, input)
        case None => out.println("No data returned")
      }

      case KQLResult(topic, fields, values, runTimeMillis) =>
        if (values.isEmpty) out.println("No data returned")
        else {
          if (nonInteractiveMode) {
            out.println(JsonHelper.renderJson(values, pretty = isPretty))
          }
          else {
            out.println(f"[Query completed in $runTimeMillis%.1f msec]")
            tabular.transform(fields, values) foreach out.println
          }
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
  
  private def isPretty = prettyJson || !nonInteractiveMode

  /**
    * Handles an asynchronous result
    * @param task  the given asynchronous task
    * @param input the executing command
    */
  private def handleAsyncResult(task: Future[_], input: String)(implicit ec: ExecutionContext): Unit = {
    if (nonInteractiveMode)
      handleNonInteractiveAsyncResult(task, input)
    else
      handleInteractiveAsyncResult(task, input)
  }

  /**
    * Handles an asynchronous result
    * @param task  the given asynchronous task
    * @param input the executing command
    */
  private def handleInteractiveAsyncResult(task: Future[_], input: String)(implicit ec: ExecutionContext) {
    // capture the start time
    val startTime = System.currentTimeMillis()

    // initially, wait for 10 seconds for the task to complete.
    // if it fails to complete in that time, queue it as an asynchronous job
    Try(Await.result(task, 10.seconds)) match {
      case Success(value) => handleResult(value, input)
      case Failure(_) =>
        out.println("Task is now running in the background (use 'jobs' to view)")
        val job = jobManager.createJob(startTime, task, input)
        task.onComplete {
          case Success(value) =>
            out.println(s"Job #${job.jobId} completed (use 'jobs -v ${job.jobId}' to view results)")
            handleResult(value, input)
          case Failure(e2) =>
            out.println(s"Job #${job.jobId} failed: ${e2.getMessage}")
        }
    }
  }

  /**
    * Handles an asynchronous result (blocks)
    * @param task  the given asynchronous task
    * @param input the executing command
    */
  private def handleNonInteractiveAsyncResult(task: Future[_], input: String)(implicit ec: ExecutionContext) {
    Try(Await.result(task, 1.hour)) match {
      case Success(value) =>
        handleResult(value, input)
      case Failure(e) =>
        out.println(s"Expected error: ${e.getMessage}")
    }
  }

  /**
    * Handles an asynchronous I/O result
    * @param asyncIO the given asynchronous I/O task
    * @param input   the executing command
    */
  private def handleAsyncResult(asyncIO: AsyncIO, input: String)(implicit ec: ExecutionContext) {
    val task = asyncIO.task
    if (nonInteractiveMode) handleNonInteractiveAsyncResult(task, input)
    else {
      out.println("Task is now running in the background (use 'jobs' to view)")
      val job = jobManager.createJob(asyncIO, input)
      task.onComplete {
        case Success(value) =>
          out.println()
          out.println(s"Job #${job.jobId} completed (use 'jobs -v ${job.jobId}' to view results)")
          handleResult(value, input)
        case Failure(e) =>
          out.println()
          out.println(s"Job #${job.jobId} failed: ${e.getMessage}")
      }
    }
  }

}

/**
  * Trifecta Result Handler Singleton
  * @author lawrence.daniels@gmail.com
  */
object TxResultHandler {

  case class Ok() {
    override def toString = "Ok"
  }

}
