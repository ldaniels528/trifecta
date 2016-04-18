package com.github.ldaniels528.trifecta

import java.io.File

import akka.actor._
import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContext.Implicits._
import scala.concurrent.duration.FiniteDuration

/**
 * Session Management
 * @author lawrence.daniels@gmail.com
 */
object SessionManagement {
  private[this] lazy val logger = LoggerFactory.getLogger(getClass)
  private[this] val system = ActorSystem("SessionManagement")

  // create the history persistence actor
  val historyActor = system.actorOf(Props[HistoryActor], name = "historyActor")

  // create the history collection
  val history = new History(100)

  def setupHistoryUpdates(file: File, frequency: FiniteDuration): Cancellable = {
    system.scheduler.schedule(frequency, frequency, historyActor, SaveHistory(file))
  }

  def shutdown() {
    system.shutdown()
  }

  /**
   * Session History Management Actor
   */
  class HistoryActor() extends Actor {
    def receive = {
      case SaveHistory(file) =>
        if (history.isDirty) {
          logger.debug("Saving history file...")
          history.store(file)
          history.isDirty = false
        }
      case unknown => unhandled(unknown)
    }
  }

  /**
   * Save History to disk message
   * @param file the file for which to save the history
   */
  case class SaveHistory(file: File)

}
