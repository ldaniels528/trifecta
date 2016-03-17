package com.github.ldaniels528.trifecta.modules.etl.io.trigger

import java.util.Date

import com.github.ldaniels528.trifecta.modules.etl.StoryConfig
import com.github.ldaniels528.trifecta.modules.etl.actors.TaskActorPool
import com.github.ldaniels528.trifecta.modules.etl.io.Scope
import com.github.ldaniels528.trifecta.modules.etl.io.device.OutputSource
import com.github.ldaniels528.trifecta.modules.etl.io.flow.Flow
import com.github.ldaniels528.trifecta.modules.etl.io.trigger.Trigger.IOStats
import com.github.ldaniels528.tabular.Tabular
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

/**
  * Represents an execution trigger
  * @author lawrence.daniels@gmail.com
  */
trait Trigger {
  protected val logger = LoggerFactory.getLogger(getClass)
  private val tabular = new Tabular()

  def id: String

  def destroy(): Unit

  def execute(story: StoryConfig)(implicit ec: ExecutionContext): Unit

  def process(story: StoryConfig, processFlows: Seq[(Flow, Scope)])(implicit ec: ExecutionContext) = {
    Future.sequence {
      processFlows map { case (flow, scope) =>
        implicit val myScope = scope

        // load the collection of properties into the scope
        scope ++= story.properties flatMap (_.load)

        // start the task
        logger.info(s"Starting to process flow '${flow.id}'...")
        val task = flow.execute(scope)

        task onComplete {
          case Success(_) =>
            val stats = generateStatistics(Seq((flow, scope)))
            tabular.synchronized {
              tabular.transform(stats) foreach logger.info
            }
          case Failure(e) =>
            logger.error(s"Processing failed: ${e.getMessage}", e)
        }
        task
      }
    }
  }

  protected def createScope(story: StoryConfig, flow: Flow) = {
    val scope = Scope()

    // add all of the system properties
    scope ++= System.getProperties.toSeq

    // add the built-in filters
    story.filters foreach { case (name, filter) =>
      scope.addFilter(name, filter)
    }

    // add some properties
    scope ++= Seq(
      "date()" -> (() => new Date()),
      "trigger.id" -> id,
      "trigger.type" -> getClass.getSimpleName
    )
    scope
  }

  protected def generateStatistics(processFlows: Seq[(Flow, Scope)]) = {
    processFlows.flatMap { case (flow, scope) =>
      flow.devices map { d =>
        val action = if (d.isInstanceOf[OutputSource]) "writes" else "reads"
        IOStats(
          flow = flow.id,
          source = d.id,
          action = action,
          count = d.getStatistics(scope).count,
          processTimeMsec = d.getStatistics(scope).elapsedTimeMillis,
          avgRecordsPerSec = (d.getStatistics(scope).avgRecordsPerSecond * 10).toInt / 10d)
      } sortBy (_.flow)
    }
  }

}

/**
  * Trigger Companion Object
  * @author lawrence.daniels@gmail.com
  */
object Trigger {
  lazy val taskPool = new TaskActorPool(3)

  /**
    * I/O Statistics
    */
  case class IOStats(flow: String, source: String, action: String, count: Long, processTimeMsec: Long, avgRecordsPerSec: Double)

}