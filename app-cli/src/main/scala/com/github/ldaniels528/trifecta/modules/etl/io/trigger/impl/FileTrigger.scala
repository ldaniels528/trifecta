package com.github.ldaniels528.trifecta.modules.etl.io.trigger.impl

import java.io.File
import java.nio.file.StandardWatchEventKinds._
import java.nio.file.{Path, Paths, WatchService}

import com.github.ldaniels528.trifecta.modules.etl.StoryConfig
import com.github.ldaniels528.trifecta.modules.etl.actors.FileManagementActor.ArchiveFile
import com.github.ldaniels528.trifecta.modules.etl.actors.{BroadwayActorSystem, FileManagementActor}
import com.github.ldaniels528.trifecta.modules.etl.io.trigger.Trigger
import com.github.ldaniels528.commons.helpers.OptionHelper._
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._
import scala.collection.concurrent.TrieMap
import scala.concurrent.ExecutionContext
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.util.{Failure, Success}

/**
  * File Trigger
  * @author lawrence.daniels@gmail.com
  */
case class FileTrigger(id: String, directories: Seq[FeedContainer]) extends Trigger {

  override def destroy() = {}

  override def execute(story: StoryConfig)(implicit ec: ExecutionContext) = {
    directories foreach { directory =>
      FileTrigger.register(watcherName = id, directory = new File(directory.path)) { file =>
        directory.find(file) foreach { feed =>
          logger.info(s"Processing '${file.getName}' for '$id'...")
          Trigger.taskPool ! createTask(story, directory, feed, file)
        }
      }
    }
  }

  private def createTask(story: StoryConfig, directory: FeedContainer, feed: FileFeed, incomingFile: File) = new Runnable {
    override def run() {
      process(story, createProcessFlows(story, directory, feed, incomingFile)) onComplete {
        case Success(result) =>
          (feed.archive ?? directory.archive) foreach { strategy =>
            FileManagementActor ! ArchiveFile(incomingFile, baseDirectory = new File(strategy.basePath))
          }
        case Failure(e) =>
          logger.error(s"Trigger '$id' failed: ${e.getMessage}")
      }
    }
  }

  private def createProcessFlows(story: StoryConfig, directory: FeedContainer, feed: FileFeed, incomingFile: File) = {
    feed.flows zip (feed.flows map { flow =>
      val scope = createScope(story, flow)
      scope ++= Seq(
        "flow.input.path" -> incomingFile.getCanonicalPath,
        "trigger.directory.path" -> directory.path)
      scope
    })
  }

}

/**
  * File Trigger Companion Object
  * @author lawrence.daniels@gmail.com
  */
object FileTrigger {
  private[this] val logger = LoggerFactory.getLogger(getClass)
  private[this] val registrations = TrieMap[File, Registration[_]]()
  private[this] val queue = TrieMap[File, QueuedFile[_]]()

  // continually poll for new files ...
  BroadwayActorSystem.scheduler.schedule(initialDelay = 0.seconds, interval = 1.seconds) {
    registrations foreach { case (directory, registration) =>
      checkForNewFiles(directory, registration)
    }
  }

  // continually check for files completion ...
  BroadwayActorSystem.scheduler.schedule(initialDelay = 0.seconds, interval = 5.seconds) {
    queue foreach { case (file, queuedFile) =>
      if (queuedFile.isReady) {
        queue.remove(file) foreach { qf =>
          logger.info(s"File '${file.getName}' is ready for processing...")
          // TODO an actor should perform the callback
          qf.registration.callback(file)
          ()
        }
      }
    }
  }

  def register[A](watcherName: String, directory: File)(callback: File => A) = {
    registrations.getOrElseUpdate(directory, {
      logger.info(s"$watcherName is watching for new files in '${directory.getAbsolutePath}'...")
      val path = Paths.get(directory.getAbsolutePath)
      val watcher = path.getFileSystem.newWatchService()
      val registration = Registration(watcherName, directory, path, watcher, callback)
      processPreExistingFiles(directory, registration)
      registration
    })
  }

  /**
    * Recursively schedules all files found in the given directory for processing
    * @param directory    the given directory
    * @param registration the given registration
    */
  private def processPreExistingFiles[A](directory: File, registration: Registration[A]) {
    Option(directory.listFiles) foreach { files =>
      logger.info(s"Processing ${files.length} pre-existing files...")
      files foreach {
        case f if f.isFile => notifyWhenReady(f, registration)
        case d if d.isDirectory => processPreExistingFiles(d, registration)
        case _ =>
      }
    }
  }

  /**
    * notifies the caller when the file is ready
    * @param file         the given [[File]]
    * @param registration the given registration
    */
  private def notifyWhenReady[A](file: File, registration: Registration[A]) {
    queue.putIfAbsent(file, QueuedFile(file, registration))
    ()
  }

  private def checkForNewFiles[A](directory: File, registration: Registration[A]) = {
    Option(registration.watcher.poll()) foreach { watchKey =>
      val events = watchKey.pollEvents()
      if (events.nonEmpty) {
        for (event <- events) {
          if (event.kind() == ENTRY_CREATE || event.kind() == ENTRY_MODIFY) {
            // get a reference to the new file
            val file = new File(directory, event.context().toString)
            logger.info(s"Waiting to consume '${file.getName}' (${directory.getAbsolutePath})...")
            notifyWhenReady(file, registration)
          }
        }
      }
    }
  }

  /**
    * Represents a queued file
    */
  case class QueuedFile[A](file: File, registration: Registration[A]) {
    // capture the file's initial state
    private var state0 = FileChangeState(file.lastModified(), file.length())

    /**
      * Attempts to determine whether the file is complete or not
      * @return true, if the file's size or last modified time hasn't changed in [up to] 10 seconds
      */
    def isReady: Boolean = {
      if (state0.elapsed < 1.second.toMillis) false
      else {
        // get the last modified time and file size
        val state1 = FileChangeState(file.lastModified(), file.length())

        // has the file changed?
        val unchanged = state0.time == state1.time && state0.size == state1.size
        if (!unchanged) {
          state0 = state1.copy(lastChange = System.currentTimeMillis())
        }

        // return the result
        state0.elapsed >= 15.seconds.toMillis && unchanged
      }
    }
  }

  case class FileChangeState(time: Long, size: Long, lastChange: Long = System.currentTimeMillis()) {
    def elapsed = System.currentTimeMillis() - lastChange
  }

  /**
    * Represents a file watching registration
    * @param watcherName the given unique registration ID
    * @param directory   the directory to watch
    * @param path        the path to watch
    * @param watcher     the [[WatchService watch service]]
    * @param callback    the callback
    * @tparam A the callback return type
    */
  case class Registration[A](watcherName: String, directory: File, path: Path, watcher: WatchService, callback: File => A) {
    Seq(ENTRY_CREATE, ENTRY_MODIFY) foreach (path.register(watcher, _))
  }

}
