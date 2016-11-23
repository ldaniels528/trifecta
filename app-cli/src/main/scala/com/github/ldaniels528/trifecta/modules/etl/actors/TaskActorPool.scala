package com.github.ldaniels528.trifecta.modules.etl.actors

import java.util.concurrent.Callable

import akka.actor.Props
import akka.pattern.ask
import akka.routing.RoundRobinPool
import akka.util.Timeout
import com.github.ldaniels528.trifecta.modules.etl.actors.TaskActor.Die
import org.slf4j.LoggerFactory

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.reflect.ClassTag

/**
  * Task Actor Pool
  * @author lawrence.daniels@gmail.com
  */
class TaskActorPool(concurrency: Int) {
  private val logger = LoggerFactory.getLogger(getClass)
  private val taskActor = BroadwayActorSystem.system.actorOf(Props[TaskActor].withRouter(RoundRobinPool(nrOfInstances = concurrency)))

  def !(message: Runnable) = taskActor ! message

  def ?[T](message: Callable[T])(implicit ec: ExecutionContext, timeout: Timeout, tag: ClassTag[T]) = (taskActor ? message).mapTo[T]

  def die(maxWait: FiniteDuration)(implicit ec: ExecutionContext) = {
    implicit val timeout: Timeout = maxWait
    for {
      _ <- Future.sequence(for (_ <- 1 to concurrency) yield taskActor ? Die)
      done <- Future {
        logger.info(s"All actors ($concurrency) have completed")
      }
    } yield done
  }

}
