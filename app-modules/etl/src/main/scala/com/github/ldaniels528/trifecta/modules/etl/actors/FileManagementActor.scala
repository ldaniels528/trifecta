package com.github.ldaniels528.trifecta.modules.etl.actors

import java.io.File
import java.text.SimpleDateFormat
import java.util.Date

import akka.actor.{Actor, ActorLogging, Props}
import akka.routing.RoundRobinPool
import com.github.ldaniels528.trifecta.modules.etl.actors.FileManagementActor.{ArchiveFile, MoveFile}
import com.github.ldaniels528.trifecta.messages.codec.gzip.GzipCompression

/**
  * File Management Actor
  * @author lawrence.daniels@gmail.com
  */
class FileManagementActor extends Actor with ActorLogging with GzipCompression {

  override def receive = {
    case ArchiveFile(file, baseDirectory) =>
      move(file, getArchiveDirectory(baseDirectory))

    case MoveFile(file, directory) =>
      move(file, directory)

    case message =>
      log.warning(s"Unhandled message '$message' (${Option(message).map(_.getClass.getName).orNull})")
      unhandled(message)
  }

  private def move(file: File, directory: File): Unit = {
    log.info(s"Moving '${file.getName}' to '${directory.getAbsolutePath}'")
    val destination = new File(directory, file.getName)

    if (!directory.exists() && !directory.mkdirs()) {
      log.warning(s"Failed to create non-existent directory ${directory.getAbsolutePath}")
    }

    if (!file.renameTo(destination)) {
      log.error(s"File '${file.getName}' could not be moved to '${directory.getAbsolutePath}'")
    }
  }

  private def getArchiveDirectory(baseDirectory: File) = {
    val formats = Seq("yyyyMMdd", "hhmmss")
    val pathElements = formats map (format => new SimpleDateFormat(format).format(new Date()))
    pathElements.foldLeft[File](baseDirectory) { (directory, pathElement) =>
      new File(directory, pathElement)
    }
  }

}

/**
  * File Management Actor Companion Object
  * @author lawrence.daniels@gmail.com
  */
object FileManagementActor {
  private val actors = BroadwayActorSystem.system.actorOf(Props[FileManagementActor].withRouter(RoundRobinPool(nrOfInstances = 5)))

  def !(message: Any) = actors ! message

  case class MoveFile(file: File, directory: File)

  case class ArchiveFile(file: File, baseDirectory: File)

}