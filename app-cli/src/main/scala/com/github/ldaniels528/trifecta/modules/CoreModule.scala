package com.github.ldaniels528.trifecta.modules

import java.io.{File, PrintStream}
import java.text.SimpleDateFormat
import java.util.concurrent.atomic.AtomicLong
import java.util.{Date, TimeZone}

import com.github.ldaniels528.trifecta.JobManager.{AsyncIOJob, JobItem}
import com.github.ldaniels528.trifecta._
import com.github.ldaniels528.trifecta.command._
import com.github.ldaniels528.trifecta.io._
import com.github.ldaniels528.trifecta.io.avro.{AvroFileInputSource, AvroFileOutputSource}
import com.github.ldaniels528.trifecta.io.json.{JSONFileInputSource, JSONFileOutputSource}
import com.github.ldaniels528.trifecta.util.ParsingHelper._
import com.github.ldaniels528.commons.helpers.PathHelper._
import com.github.ldaniels528.commons.helpers.ResourceHelper._
import com.github.ldaniels528.commons.helpers.StringHelper._
import org.apache.commons.io.IOUtils

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent._
import scala.io.Source
import scala.language.postfixOps
import scala.util.{Properties, Try}

/**
 * Core Module
 * @author lawrence.daniels@gmail.com
 */
class CoreModule(config: TxConfig) extends Module {
  private val out: PrintStream = config.out

  // define the process parsing regular expression
  private val PID_MacOS_r = "^\\s*(\\d+)\\s*(\\d+)\\s*(\\d+)\\s*(\\S+)\\s*(\\S+)\\s*(\\S+)\\s*(\\S+)\\s*(.*)".r
  private val PID_Linux_r = "^\\s*(\\S+)\\s*(\\d+)\\s*(\\d+)\\s*(\\d+)\\s*(\\S+)\\s*(\\S+)\\s*(\\S+)\\s*(\\S+)\\s*(.*)".r
  private val NET_STAT_r = "^\\s*(\\S+)\\s*(\\S+)\\s*(\\S+)\\s*(\\S+)\\s*(\\S+)\\s*(\\S+)\\s*(.*)".r

  override def getCommands(implicit rt: TxRuntimeContext): Seq[Command] = Seq(
    Command(this, "!", executeHistory, UnixLikeParams(Seq("!" -> false, "?" -> false, "index|count" -> false), Nil), help = "Executes a previously issued command"),
    Command(this, "?", help, UnixLikeParams(Seq("search-term" -> false), Seq("-m" -> "moduleName")), help = "Provides the list of available commands"),
    Command(this, "autoswitch", autoSwitch, UnixLikeParams(Seq("state" -> false)), help = "Automatically switches to the module of the most recently executed command"),
    Command(this, "cat", cat, UnixLikeParams(Seq("file" -> true), Nil), help = "Dumps the contents of the given file", promptAware = true),
    Command(this, "cd", changeDir, UnixLikeParams(Seq("path" -> true)), help = "Changes the local file system path/directory", promptAware = true),
    Command(this, "charset", charSet, UnixLikeParams(Seq("encoding" -> false)), help = "Retrieves or sets the character encoding"),
    Command(this, "columns", columnWidthGetOrSet, UnixLikeParams(Seq("columnWidth" -> false)), help = "Retrieves or sets the column width for message output"),
    Command(this, "debug", debug, UnixLikeParams(Seq("enabled" -> false)), help = "Switches debugging on/off", undocumented = true),
    Command(this, "exit", exit, UnixLikeParams(), help = "Exits the shell"),
    Command(this, "help", help, UnixLikeParams(Seq("searchTerm" -> false), Seq("-m" -> "moduleName")), help = "Provides the list of available commands"),
    Command(this, "history", listHistory, UnixLikeParams(Seq("count" -> false)), help = "Returns a list of previously issued commands"),
    Command(this, "jobs", manageJob, UnixLikeParams(Seq("jobNumber" -> false), Seq("-c" -> "clear jobs", "-d" -> "delete job", "-l" -> "list jobs", "-v" -> "result")), help = "Returns the list of currently running jobs"),
    Command(this, "ls", listFiles, UnixLikeParams(Seq("path" -> false)), help = "Retrieves the files from the current directory", promptAware = true),
    Command(this, "module", useModule, UnixLikeParams(Seq("module" -> true)), help = "Switches the active module"),
    Command(this, "modules", listModules, UnixLikeParams(), help = "Returns a list of configured modules"),
    Command(this, "ps", processList, UnixLikeParams(Seq("node" -> false), Seq("-i" -> "identityFile", "-u" -> "userName")), help = "Displays a list of \"configured\" running processes", undocumented = true),
    Command(this, "pwd", printWorkingDirectory, UnixLikeParams(), help = "Displays current working directory"),
    Command(this, "syntax", syntax, UnixLikeParams(Seq("command" -> true)), help = "Returns the syntax/usage for a given command"),
    Command(this, "systime", systemTime, UnixLikeParams(Seq("date" -> false)), help = "Returns the system time as an EPOC in milliseconds"),
    Command(this, "time", time, UnixLikeParams(Seq("sysTime" -> false)), help = "Returns the system time"),
    Command(this, "timeutc", timeUTC, UnixLikeParams(Seq("sysTime" -> false)), help = "Returns the system time in UTC"),
    Command(this, "undoc", listUndocumented, UnixLikeParams(), help = "Displays undocumented commands", undocumented = true),
    Command(this, "version", version, UnixLikeParams(), help = "Returns the application version"),
    Command(this, "wget", httpGet, UnixLikeParams(Seq("url" -> true)), help = "Retrieves remote content via HTTP"))

  /**
   * Returns an input source
   * @param url the given input URL (e.g. "file:avro:/tmp/messages.avro")
   * @return the option of an input source
   */
  override def getInputSource(url: String): Option[InputSource] = {
    url match {
      case s if s.startsWith("file:avro:") => url.extractProperty("file:avro:") map (AvroFileInputSource(_))
      case s if s.startsWith("file:json:") => url.extractProperty("file:json:") map (JSONFileInputSource(_))
      case s if s.startsWith("file:text:") => url.extractProperty("file:text:") map (TextFileInputSource(_))
      case s if s.startsWith("http:") => Option(new HttpInputSource(url))
      case _ => None
    }
  }

  /**
   * Returns an output source
   * @param url the given output URL (e.g. "file:avro:/tmp/messages.avro")
   * @return the option of an output source
   */
  override def getOutputSource(url: String): Option[OutputSource] = {
    url match {
      case s if s.startsWith("file:avro:") => url.extractProperty("file:avro:") map (AvroFileOutputSource(_))
      case s if s.startsWith("file:json:") => url.extractProperty("file:json:") map (JSONFileOutputSource(_))
      // TODO http: for file upload
      case _ => None
    }
  }

  /**
   * Returns the name of the module (e.g. "kafka")
   * @return the name of the module
   */
  override def moduleName = "core"

  /**
   * Returns the label of the module (e.g. "kafka")
   * @return the label of the module
   */
  override def moduleLabel = "core"

  override def prompt: String = config.cwd

  override def shutdown() = ()

  override def supportedPrefixes: Seq[String] = Seq("file", "http")

  // load the commands from the modules
  private def commandSet(implicit rt: TxRuntimeContext): Map[String, Command] = rt.moduleManager.commandSet

  /**
   * Automatically switches to the module of the most recently executed command
   * @example autoswitch true
   */
  def autoSwitch(params: UnixLikeArgs): String = {
    params.args.headOption map (_.toBoolean) foreach (config.autoSwitching = _)
    s"auto switching is ${if (config.autoSwitching) "On" else "Off"}"
  }

  /**
   * Displays the contents of the given file
   * @example cat "avro/schema1.avsc"
   */
  def cat(params: UnixLikeArgs): Seq[String] = {
    import scala.io.Source

    // get the file path
    params.args.headOption match {
      case Some(path) => Source.fromFile(expandPath(path)).getLines().toSeq
      case None => dieSyntax(params)
    }
  }

  /**
   * Changes the local file system path/directory
   * @example cd "/home/ldaniels/examples"
   */
  def changeDir(params: UnixLikeArgs): Option[String] = {
    val cwd = params.args.headOption map {
      case path if path == ".." =>
        config.cwd.split("[/]") match {
          case a if a.length <= 1 => "/"
          case a =>
            val newPath = a.init.mkString("/")
            if (newPath.trim.length == 0) "/" else newPath
        }
      case path => setupPath(path)
    }

    cwd foreach { path =>
      config.cwd = path
    }
    cwd
  }

  /**
   * Retrieves or sets the character encoding
   * @example charset "UTF-8"
   */
  def charSet(params: UnixLikeArgs): Either[Unit, String] = {
    params.args.headOption match {
      case Some(newEncoding) => Left(config.encoding = newEncoding)
      case None => Right(config.encoding)
    }
  }

  /**
   * Retrieves or sets the column width for message output
   * @example columns 30
   */
  def columnWidthGetOrSet(params: UnixLikeArgs): Either[Unit, Int] = {
    params.args.headOption match {
      case Some(arg) => Left(config.columns = parseInt("columnWidth", arg))
      case None => Right(config.columns)
    }
  }

  /**
   * Toggles the current debug state
   * @param params the given command line arguments
   * @return the current state ("On" or "Off")
   */
  def debug(params: UnixLikeArgs): String = {
    if (params.args.isEmpty) config.debugOn = !config.debugOn else config.debugOn = params.args.head.toBoolean
    s"debugging is ${if (config.debugOn) "On" else "Off"}"
  }

  /**
   * History execution command. This command can either executed a
   * previously executed command by its unique identifier, or list (!?) all previously
   * executed commands.
   * @example !123
   * @example !?10
   * @example !?
   */
  def executeHistory(params: UnixLikeArgs)(implicit rt: TxRuntimeContext) {
    for {
      command <- params.args match {
        case Nil => SessionManagement.history.last
        case "!" :: Nil => SessionManagement.history.last
        case "?" :: Nil => Some("history")
        case "?" :: count :: Nil => Some(s"history $count")
        case index :: Nil if index.matches("\\d+") => SessionManagement.history(parseInt("history ID", index) - 1)
        case _ => dieSyntax(params)
      }
    } {
      out.println(s">> $command")
      val result = rt.interpret(command)
      rt.handleResult(result, command)
    }
  }

  /**
   * Exits the shell
   * @example exit
   */
  def exit(params: UnixLikeArgs) {
    config.alive = false
    SessionManagement.history.store(TxConfig.historyFile)
  }

  /**
   * Provides the list of available commands
   * @example ?
   * @example ?k
   * @example help
   */
  def help(params: UnixLikeArgs)(implicit rt: TxRuntimeContext): Seq[CommandItem] = {
    // get the prefix
    val prefix = params.args.headOption

    // was the module switch used?
    val commands = params("-m") match {
      case Some(moduleName) => commandSet.toSeq filter { case (name, cmd) => cmd.module.moduleName == moduleName}
      case None => commandSet.toSeq
    }

    // provide help for each commands
    commands filter {
      case (nameA, cmdA) => !cmdA.undocumented && (prefix.isEmpty || prefix.exists(nameA.startsWith))
    } sortBy (_._1) map {
      case (nameB, cmdB) => CommandItem(nameB, cmdB.module.moduleName, cmdB.help)
    }
  }

  /**
   * Retrieves remote content via HTTP
   * @example wget "http://www.example.com/"
   * @example wget -f json "http://localhost:9000/api/tasks"
   */
  def httpGet(params: UnixLikeArgs): Option[Any] = {
    import java.io.ByteArrayOutputStream
    import java.net._

    // retrieve (or guess) the value's format
    val valueType = params("-f", "bytes")

    // download the content from the remote peer
    val bytes = params.args.headOption map { urlString =>
      new URL(urlString).openConnection().asInstanceOf[HttpURLConnection] use { conn =>
        conn.getInputStream use { in =>
          val out = new ByteArrayOutputStream(1024)
          IOUtils.copy(in, out)
          out.toByteArray
        }
      }
    }

    // return either the byte array or the decoded value
    bytes map (decodeValue(_, valueType))
  }

  /**
   * Retrieves the files from the current directory
   * @example ls
   * @example ls avro
   */
  def listFiles(params: UnixLikeArgs): Option[Seq[String]] = {
    // get the optional path argument
    val path: String = params.args.headOption map expandPath map setupPath getOrElse config.cwd

    // perform the action
    Option(new File(path).list) map { files =>
      files map { file =>
        if (file.startsWith(path)) file.substring(path.length) else file
      }
    }
  }

  /**
   * Retrieves previously executed commands
   * @example history
   */
  def listHistory(params: UnixLikeArgs): Seq[HistoryItem] = {
    val count = params.args.headOption map (parseInt("count", _))
    val lines = SessionManagement.history.getLines(count.getOrElse(-1))
    ((1 to lines.size) zip lines) map {
      case (itemNo, command) => HistoryItem(itemNo, command)
    }
  }

  /**
   * Job management: view, list or remove jobs
   * @example jobs 1234
   * @example jobs -c
   * @example jobs -d 1234
   * @example jobs -v 1234
   */
  def manageJob(params: UnixLikeArgs): Any = {
    val jobMgr = config.jobManager

    // list a specific job?
    params.args.headOption map parseJobId map { myJobId =>
      jobMgr.getJobs filter (_.jobId == myJobId) map toJobDetail
    } getOrElse {
      // delete a job by ID?
      if ((params("-d") map parseJobId map jobMgr.killJobById).isDefined) "Ok"

      // retrieve the job's value?
      else if (params.contains("-v")) {
        val jobId = params("-v") map parseJobId getOrElse die(s"${params.commandName.get} -v jobId")
        jobMgr.getJobById(jobId) match {
          case Some(aio@AsyncIOJob(_, asyncIO, _)) =>
            val task = asyncIO.task
            if (task.isCompleted) asyncIO.getCount else toJobDetail(aio)
          case Some(job) =>
            val task = job.task
            if (task.isCompleted) task.value else toJobDetail(job)
          case None =>
            die(s"Job #$jobId not found")
        }
      }

      // clear all jobs?
      else if (params.contains("-w")) {
        jobMgr.clear()
        "Ok"
      }

      // return all jobs
      else config.jobManager.getJobs map toJobDetail
    }
  }

  /**
   * Returns the list of modules
   * @example modules
   */
  def listModules(params: UnixLikeArgs)(implicit rt: TxRuntimeContext): Seq[ModuleItem] = {
    val activeModule = rt.moduleManager.activeModule
    rt.moduleManager.modules.map(m =>
      ModuleItem(m.moduleName, m.getClass.getName, loaded = true, activeModule.exists(_.moduleName == m.moduleName)))
      .sortBy(_.name)
  }

  /**
   * List undocumented commands
   * @example undoc
   * @example undoc sdeploy
   */
  def listUndocumented(params: UnixLikeArgs)(implicit rt: TxRuntimeContext): Seq[CommandItem] = {
    val args = params.args
    commandSet.toSeq filter {
      case (nameA, cmdA) => cmdA.undocumented && (args.isEmpty || nameA.startsWith(args.head))
    } sortBy (_._1) map {
      case (nameB, cmdB) => CommandItem(nameB, cmdB.module.moduleName, cmdB.help)
    }
  }

  /**
   * Display a list of "configured" running processes
   * @example ps
   * @example ps -u ldaniels dev528
   * @example ps -i /home/ubuntu/aws.pem -u ubuntu dev528
   */
  def processList(params: UnixLikeArgs): Future[Seq[String]] = {
    import scala.util.Properties

    // this command only works on Linux
    if (Properties.isMac || Properties.isWin) die("Unsupported platform for this command")

    // get the node
    val node = extract(params.args, 0) getOrElse "."
    out.println(s"Gathering process info from host: ${if (node == ".") "localhost" else node}")

    // parse the process and port mapping data
    for {
    // retrieve the process and port map data
      (psData, portMap) <- parseNetStatData(node, params)

      // filter the data, and produce the results
      result = psData filter (s => s.contains("mysqld") || s.contains("java") || s.contains("python")) flatMap {
        case PID_MacOS_r(user, pid, _, _, time1, _, time2, cmd, fargs) => Some(parsePSData(pid, cmd, fargs, portMap.get(pid)))
        case PID_Linux_r(user, pid, _, _, time1, _, time2, cmd, fargs) => Some(parsePSData(pid, cmd, fargs, portMap.get(pid)))
        case _ => None
      }
    } yield result
  }

  /**
   * Parses process data produced by the UNIX "ps" command
   */
  private def parsePSData(pid: String, cmd: String, args: String, portCmd: Option[String]): String = {
    val command = cmd match {
      case s if s.contains("mysqld") => "MySQL Server"
      case s if s.endsWith("java") =>
        args match {
          case a if a.contains("cassandra") => "Cassandra"
          case a if a.contains("kafka") => "Kafka"
          case a if a.contains("mysqld") => "MySQLd"
          case a if a.contains("storm nimbus") => "Storm Nimbus"
          case a if a.contains("storm supervisor") => "Storm Supervisor"
          case a if a.contains("storm ui") => "Storm UI"
          case a if a.contains("storm") => "Storm"
          case a if a.contains("org.elasticsearch.bootstrap.Elasticsearch") => "ElasticSearch"
          case a if a.contains("trifecta.jar") => "Trifecta"
          case a if a.contains("/usr/local/java/zookeeper") => "Zookeeper"
          case _ => s"java [$args]"
        }
      case s =>
        args match {
          case a if a.contains("storm nimbus") => "Storm Nimbus"
          case a if a.contains("storm supervisor") => "Storm Supervisor"
          case a if a.contains("storm ui") => "Storm UI"
          case _ => s"$cmd [$args]"
        }
    }

    portCmd match {
      case Some(port) => f"$pid%6s $command <$port>"
      case _ => f"$pid%6s $command"
    }
  }

  /**
   * Retrieves "netstat -ptln" and "ps -ef" data from a remote node
   * @param node the given remote node (e.g. "Verify")
   * @return a future containing the data
   */
  private def parseNetStatData(node: String, params: UnixLikeArgs): Future[(Seq[String], Map[String, String])] = {
    import scala.sys.process._

    // get the user name
    val userName = params("-u") getOrElse Properties.userName

    // get the optional identity file (.pem)
    val identityFile = params("-i") map (pem => s"-i $pem") getOrElse ""

    // asynchronously get the raw output from 'ps -ef'
    val psdataF: Future[Seq[String]] = Future {
      Source.fromString((node match {
        case "." => "ps -ef"
        case host => s"ssh $identityFile $userName@$host ps -ef"
      }).!!).getLines().toSeq
    }

    // asynchronously get the port mapping
    val portmapF: Future[Map[String, String]] = Future {
      // get the lines of data from 'netstat'
      val netStat = Source.fromString((node match {
        case "." if Properties.isMac => "netstat -gilns"
        case "." => "netstat -ptln"
        case host => s"ssh $identityFile $userName@$host netstat -ptln"
      }).!!).getLines().toSeq.tail

      // build the port mapping
      netStat flatMap {
        case NET_STAT_r(_, _, _, rawport, _, _, pidcmd, _*) =>
          if (pidcmd.contains("java")) {
            val port = rawport.substring(rawport.lastIndexOf(':') + 1)
            val Array(pid, cmd) = pidcmd.trim.split("[/]")
            Some((port, pid, cmd))
          } else None
        case _ => None
      } map {
        case (port, pid, cmd) => pid -> port
      } groupBy (_._1) map {
        case (pid, seq) => (pid, seq.sortBy(_._2.toInt) map (_._2) mkString ", ")
      }
    }

    // let's combine the futures
    for {
      pasdata <- psdataF
      portmap <- portmapF
    } yield (pasdata, portmap)
  }

  /**
   * Print the current working directory
   * @example pwd
   */
  def printWorkingDirectory(args: UnixLikeArgs) = new File(config.cwd).getCanonicalPath

  /**
   * Returns the usage for a command
   * @example syntax time
   */
  def syntax(params: UnixLikeArgs)(implicit rt: TxRuntimeContext): Seq[String] = {
    val commandName = params.args.head

    rt.moduleManager.findCommandByName(commandName) match {
      case Some(command) => Seq(s"Description: ${command.help}", s"Usage: ${command.prototype}")
      case None => die(s"Command '$commandName' not found")
    }
  }

  /**
   * Returns the system time as an EPOC in milliseconds
   * @example systime                     => 1411439360907
   * @example systime 2014-04-15T12:00:00 => 1397545920000
   */
  def systemTime(params: UnixLikeArgs): Long = {
    params.args match {
      case date :: Nil =>
        val fmt = new SimpleDateFormat("yyyy-MM-dd'T'hh:mm:ss")
        Try(fmt.parse(date)) map (_.getTime) getOrElse die("Expected date format is \"yyyy-MM-dd'T'hh:mm:ss\"")
      case Nil => System.currentTimeMillis()
      case _ => dieSyntax(params)
    }
  }

  /**
   * Returns the time in the local time zone
   * @example time               => "Mon Sep 22 18:11:07 PDT 2014"
   * @example time 1410937200000 => "Wed Sep 17 00:00:00 PDT 2014"
   */
  def time(params: UnixLikeArgs): Date = {
    params.args match {
      case sysTime :: Nil if sysTime.matches("\\d+") => new Date(sysTime.toLong)
      case Nil => new Date()
      case _ => dieSyntax(params)
    }
  }

  /**
   * Returns the time in the GMT time zone
   * @example timeutc               => Tue Sep 23 02:27:40 GMT 2014
   * @example timeutc 1410937200000 => Wed Sep 17 07:00:00 GMT 2014
   */
  def timeUTC(params: UnixLikeArgs): String = {
    // get the date
    val date = params.args match {
      case sysTime :: Nil if sysTime.matches("\\d+") => new Date(sysTime.toLong)
      case Nil => new Date()
      case _ => dieSyntax(params)
    }

    // format the date in GMT
    val fmt = new SimpleDateFormat("E MMM dd HH:mm:ss z yyyy")
    fmt.setTimeZone(TimeZone.getTimeZone("GMT"))
    fmt.format(date)
  }

  /**
   * Switches the active module
   * @example use kafka
   */
  def useModule(params: UnixLikeArgs)(implicit rt: TxRuntimeContext) {
    val moduleName = params.args.head
    rt.moduleManager.findModuleByName(moduleName) match {
      case Some(module) => rt.moduleManager.activeModule = module
      case None => die(s"Module '$moduleName' not found")
    }
  }

  /**
   * "version" - Returns the application version
   * @return the application version
   */
  def version(args: UnixLikeArgs): String = TrifectaShell.VERSION

  private def parseJobId(id: String): Int = parseInt("job number", id)

  private def setupPath(key: String): String = {
    val cwd = config.cwd
    key match {
      case s if s.startsWith("/") => key
      case s => (if (cwd.endsWith("/")) cwd else cwd + "/") + s
    }
  }

  private[trifecta] def toJobDetail(job: JobItem): JobDetail = {
    val myTask = job.task
    val jobStatus: String = if (myTask.isCompleted) "Completed" else "Running"
    val jobElapsedTime: Option[Double] = {
      val millis: Option[Long] =
        if (myTask.isCompleted) Option(job.endTime) map (_ - job.startTime)
        else Option(job.startTime) map (System.currentTimeMillis() - _)
      millis map (t => Math.round(10d * (t.toDouble / 1000d)) / 10d)
    }

    job match {
      case AsyncIOJob(jobId, asyncIO, command) =>
        JobDetail(
          jobId = job.jobId,
          status = jobStatus,
          runTimeSecs = jobElapsedTime,
          read = Option(asyncIO.counter.read),
          written = Option(asyncIO.counter.written),
          command = job.command)
      case _ =>
        JobDetail(
          jobId = job.jobId,
          status = jobStatus,
          runTimeSecs = jobElapsedTime,
          read = None,
          written = None,
          command = job.command)
    }
  }

  case class CommandItem(command: String, module: String, description: String)

  case class HistoryItem(uid: Int, command: String)

  case class JobDetail(jobId: Int, read: Option[AtomicLong], written: Option[AtomicLong], runTimeSecs: Option[Double], status: String, command: String)

  case class ModuleItem(name: String, className: String, loaded: Boolean, active: Boolean)

  case class ScopeItem(name: String, module: String, `type`: String, value: Option[_] = None)

}
