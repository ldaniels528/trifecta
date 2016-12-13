package com.github.ldaniels528.trifecta.modules.etl.actors

import java.util.concurrent.Callable

import akka.actor.{Actor, ActorLogging}
import com.github.ldaniels528.trifecta.modules.etl.actors.TaskActor.{Dead, Die}

/**
  * Task Actor
  * @author lawrence.daniels@gmail.com
  */
class TaskActor() extends Actor with ActorLogging {

  override def receive = {
    case task: Runnable =>
      task.run()

    case task: Callable[_] =>
      val mySender = sender
      val value = task.call()
      mySender ! value

    case Die =>
      sender ! Dead
    //context.stop(self)

    case message =>
      log.warning(s"Unhandled message '$message' (${Option(message).map(_.getClass.getName).orNull})")
      unhandled(message)
  }

}

/**
  * Task Actor Companion Object
  * @author lawrence.daniels@gmail.com
  */
object TaskActor {

  case object Die

  case object Dead

}