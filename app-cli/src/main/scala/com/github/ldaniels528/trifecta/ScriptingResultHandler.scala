package com.github.ldaniels528.trifecta

import com.github.ldaniels528.trifecta.CommandLineHelper._
import com.github.ldaniels528.trifecta.io.AsyncIO
import com.github.ldaniels528.trifecta.messages.codec.json.JsonHelper
import net.liftweb.json.JValue
import org.slf4j.LoggerFactory
import play.api.libs.json.{JsValue, Json}

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext}
import scala.util.{Failure, Success, Try}

/**
  * Scripting Result Handler
  * @author lawrence.daniels@gmail.com
  */
class ScriptingResultHandler(config: TxConfig, jobManager: JobManager, args: Array[String]) extends TxResultHandler {
  private lazy val logger = LoggerFactory.getLogger(getClass)
  private val isPrettyJson = args.isPrettyJson
  private val out = config.out

  /**
    * Handles the processing and/or display of the given result of a command execution
    * @param result the given result
    * @param ec     the given execution context
    */
  override def handleResult(result: Any, input: String)(implicit ec: ExecutionContext): Unit = {
    result match {
      // filter out any undesired results
      case a if a == null | a.isInstanceOf[Unit] | a == None | a == Nil => out.println()

      // handle the asynchronous I/O cases
      case a: AsyncIO => handleAsyncResult(a, input)

      // intercept intermediate results
      case e: Either[_, _] => e match {
        case Left(v) => handleResult(v, input)
        case Right(v) => handleResult(v, input)
      }
      case o: Option[_] => o foreach (handleResult(_, input))

      // handle JSON values
      case js: JValue => out.println(JsonHelper.renderJson(js, pretty = isPrettyJson))
      case js: JsValue => out.println(Json.prettyPrint(js))

      // handle lists, vectors and sequences
      case x: AnyRef => handleResult(JsonHelper.transformFrom(x), input)

      // unhandled ...
      case x =>
        if(config.debugOn) {
          logger.info(s"$x (${Option(x).map(_.getClass.getName)}")
        }
        out.println()
    }
  }

  /**
    * Handles an asynchronous I/O result
    * @param asyncIO the given asynchronous I/O task
    * @param input   the executing command
    */
  private def handleAsyncResult(asyncIO: AsyncIO, input: String)(implicit ec: ExecutionContext) {
    val task = asyncIO.task
    Try(Await.result(task, 1.hour)) match {
      case Success(value) =>
        handleResult(value, input)
      case Failure(e) =>
        out.println(s"Expected error: ${e.getMessage}")
    }
  }

}
