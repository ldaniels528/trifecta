package com.github.ldaniels528.trifecta

import java.util.Random
import java.util.concurrent.atomic.AtomicInteger

import com.github.ldaniels528.trifecta.JobManager._
import com.github.ldaniels528.trifecta.io.AsyncIO

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

/**
 * Background Job/Task Manager
 * @author lawrence.daniels@gmail.com
 */
class JobManager() {
  private val jobIdGen = new AtomicInteger(new Random().nextInt(1000) + 100)
  private val jobs = mutable.Map[Int, JobItem]()

  /**
   * Removes all jobs
   */
  def clear() = jobs.clear()

  /**
   * Creates a new asynchronous job
   * @param asyncIO the asynchronous I/O job
   * @param command the command that resulted in an asynchronous task
   * @return a new job
   */
  def createJob(asyncIO: AsyncIO, command: String)(implicit ec: ExecutionContext): JobItem = {
    registerJob(AsyncIOJob(jobId = nextJobID, asyncIO, command))
  }

  /**
   * Creates a new asynchronous job
   * @param startTime the start time of the job
   * @param task the asynchronous task
   * @param command the command that resulted in an asynchronous task
   * @return a new job
   */
  def createJob(startTime: Long, task: Future[_], command: String)(implicit ec: ExecutionContext): JobItem = {
    registerJob(AsyncJob(jobId = nextJobID, startTime, task, command))
  }

  /**
   * Retrieves a job by ID
   * @param jobId the given job ID
   * @return the option of a job
   */
  def getJobById(jobId: Int): Option[JobItem] = jobs.get(jobId)

  /**
   * Retrieves a job's task by ID
   * @param jobId the given job ID
   * @return the option of a job
   */
  def getJobTaskById(jobId: Int): Option[Future[_]] = jobs.get(jobId) map (_.task)

  /**
   * Retrieves the complete collection of jobs
   * @return the collection of jobs
   */
  def getJobs: Seq[JobItem] = jobs.values.toSeq

  /**
   * Kills a job by ID
   * @param jobId the given job ID
   */
  def killJobById(jobId: Int): Unit = {
    jobs -= jobId
    ()
  }

  /**
   * Returns the next unique job ID
   * @return the next job ID
   */
  private def nextJobID: Int = jobIdGen.incrementAndGet()

  /**
   * Registers a job for processing
   * @param job the given [[JobItem]]
   * @return the registered job
   */
  private def registerJob(job: JobItem)(implicit ec: ExecutionContext): JobItem = {
    // trigger the update for the end time
    job.task.onComplete {
      case Success(v) => job.endTime = System.currentTimeMillis()
      case Failure(_) => job.endTime = System.currentTimeMillis()
    }

    // add the job to the mapping
    jobs += (job.jobId -> job)
    job
  }

}

/**
 * Job Manager Companion Object
 * @author lawrence.daniels@gmail.com
 */
object JobManager {

  /**
   * Represents a job
   */
  trait JobItem {

    def jobId: Int

    def command: String

    def task: Future[_]

    def startTime: Long

    var endTime: Long = _

  }

  /**
   * Represents an asynchronous job
   */
  case class AsyncJob(jobId: Int, startTime: Long, task: Future[_], command: String) extends JobItem

  /**
   * Represents an asynchronous I/O job
   */
  case class AsyncIOJob(jobId: Int, asyncIO: AsyncIO, command: String) extends JobItem {

    def task: Future[_] = asyncIO.task

    def startTime: Long = asyncIO.startTime

  }

}