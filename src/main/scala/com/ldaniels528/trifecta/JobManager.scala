package com.ldaniels528.trifecta

import java.util.Random
import java.util.concurrent.atomic.{AtomicLong, AtomicInteger}

import com.ldaniels528.trifecta.JobManager._

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

/**
 * Job Manager
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class JobManager() {
  private val jobIdGen = new AtomicInteger(new Random().nextInt(1000) + 1000)
  private val jobs = mutable.Map[Int, JobItem]()

  /**
   * Removes all jobs
   */
  def clear() = jobs.clear()

  /**
   * Creates a new asynchronous job
   * @param startTime the start time of the job
   * @param f the asynchronous task
   * @param command the command that resulted in an asynchronous task
   * @return  a new job
   */
  def createJob(startTime: Long, f: Future[_], command: String)(implicit ec: ExecutionContext): JobItem = {
    // create the job
    val job = JobItem(
      jobId = jobIdGen.incrementAndGet(),
      startTime = startTime,
      task = f,
      command = command)

    // trigger the update for the end time
    f.onComplete {
      case Success(v) => job.endTime.set(System.currentTimeMillis())
      case Failure(_) => job.endTime.set(System.currentTimeMillis())
    }

    // add the job to the mapping
    jobs += (job.jobId -> job)
    job
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
    jobs -= jobId; ()
  }

}

/**
 * Job Manager Companion Object
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
object JobManager {

  case class JobItem(jobId: Int, startTime: Long, task: Future[_], command: String) {
    val endTime = new AtomicLong(0L)
  }

}