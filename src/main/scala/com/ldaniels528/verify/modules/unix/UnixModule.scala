package com.ldaniels528.verify.modules.unix

import java.io.{File, PrintStream}
import java.util.{Date, TimeZone}

import com.ldaniels528.verify.VerifyShellRuntime
import com.ldaniels528.verify.modules.Module
import com.ldaniels528.verify.modules.Module.Command

import scala.concurrent.ExecutionContext.Implicits._
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.language.postfixOps

/**
 * UNIX Systems Module
 * @author lawrence.daniels@gmail.com
 */
class UnixModule(rt: VerifyShellRuntime, out: PrintStream) extends Module {

  // define the process parsing regular expression
  private val PID_MacOS_r = "^\\s*(\\d+)\\s*(\\d+)\\s*(\\d+)\\s*(\\S+)\\s*(\\S+)\\s*(\\S+)\\s*(\\S+)\\s*(.*)".r
  private val PID_Linux_r = "^\\s*(\\S+)\\s*(\\d+)\\s*(\\d+)\\s*(\\d+)\\s*(\\S+)\\s*(\\S+)\\s*(\\S+)\\s*(\\S+)\\s*(.*)".r
  private val NET_STAT_r = "^\\s*(\\S+)\\s*(\\S+)\\s*(\\S+)\\s*(\\S+)\\s*(\\S+)\\s*(\\S+)\\s*(.*)".r

  // current working directory
  private var cwd: String = new File(".").getCanonicalPath

  val name = "unix"

  override def prompt: String = cwd

  val getCommands: Seq[Command] = Seq(
    Command(this, "cat", cat, (Seq("file"), Seq.empty), help = "Dumps the contents of the given file"),
    Command(this, "cd", changeDir, (Seq("key"), Seq.empty), help = "Changes the local file system path/directory"),
    Command(this, "hostname", hostname, help = "Returns the name of the current host"),
    Command(this, "ls", listFiles, (Seq.empty, Seq("path")), help = "Retrieves the files from the current directory"),
    Command(this, "pkill", processKill, (Seq("pid0"), Seq("pid1", "pid2", "pid3", "pid4", "pid5", "pid6")), help = "Terminates specific running processes"),
    Command(this, "ps", processList, (Seq.empty, Seq("node", "timeout")), help = "Display a list of \"configured\" running processes"),
    Command(this, "pwd", printWorkingDirectory, (Seq.empty, Seq.empty), help = "Display current working directory"),
    Command(this, "storm", stormDeploy, (Seq("jarfile", "topology"), Seq("arguments")), help = "Deploys a topology to the Storm server"),
    Command(this, "systime", systemTime, help = "Returns the system time as an EPOC in milliseconds"),
    Command(this, "time", time, help = "Returns the system time"),
    Command(this, "timeutc", timeUTC, help = "Returns the system time in UTC"))

  override def shutdown() = ()

  /**
   * Displays the contents of the given file
   * Example: cat avro/schema1.avsc
   */
  def cat(args: String*): Seq[String] = {
    import scala.io.Source

    // get the file path
    val path = expandPath(args.head)
    Source.fromFile(path).getLines().toSeq
  }

  /**
   * "cd" - Changes the local file system path/directory
   */
  def changeDir(args: String*): String = {
    // get the argument
    val key = args.head

    // perform the action
    cwd = key match {
      case s if s == ".." =>
        cwd.split("[/]") match {
          case a if a.length <= 1 => "/"
          case a =>
            val newpath = a.init.mkString("/")
            if (newpath.trim.length == 0) "/" else newpath
        }
      case s => setupPath(s)
    }
    cwd
  }

  /**
   * "ls" - Retrieves the files from the current directory
   */
  def listFiles(args: String*): Seq[String] = {
    // get the argument
    val path = if (args.nonEmpty) setupPath(args.head) else cwd

    // perform the action
    new File(path).list map { file =>
        if(file.startsWith(path)) file.substring(path.length) else file
    }
  }

  private def setupPath(key: String): String = {
    key match {
      case s if s.startsWith("/") => key
      case s => (if (cwd.endsWith("/")) cwd else cwd + "/") + s
    }
  }

  /**
   * "hostname" command - Returns the name of the current host
   */
  def hostname(args: String*): String = {
    java.net.InetAddress.getLocalHost.getHostName
  }

  /**
   * "ps" command - Display a list of "configured" running processes
   */
  def processList(args: String*): Seq[String] = {
    import scala.util.Properties

    // get the node
    val node = extract(args, 0) getOrElse "."
    val timeout = extract(args, 1) map (_.toInt) getOrElse 60
    out.println(s"Gathering process info from host: ${if (node == ".") "localhost" else node}")

    // parse the process and port mapping data
    val outcome = for {
    // retrieve the process and port map data
      (psData, portMap) <- remoteData(node)

      // process the raw output
      lines = psData map (seq => if (Properties.isMac) seq.tail else seq)

      // filter the data, and produce the results
      result = lines filter (s => s.contains("mysqld") || s.contains("java") || s.contains("python")) flatMap {
        case PID_MacOS_r(user, pid, _, _, time1, _, time2, cmd, fargs) => Some(parsePSData(pid, cmd, fargs, portMap.get(pid)))
        case PID_Linux_r(user, pid, _, _, time1, _, time2, cmd, fargs) => Some(parsePSData(pid, cmd, fargs, portMap.get(pid)))
        case _ => None
      }
    } yield result

    // and let's wait for the result...
    Await.result(outcome, timeout seconds)
  }

  /**
   * "pkill" command - Terminates specific running processes
   */
  def processKill(args: String*): String = {
    import scala.sys.process._

    // get the PIDs -- ensure they are integers
    val pidList = args map (_.toInt)

    // kill the processes
    s"kill ${pidList mkString " "}".!!
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
          case a if a.contains("tesla-stream") => "Verify"
          case a if a.contains("storm nimbus") => "Storm Nimbus"
          case a if a.contains("storm supervisor") => "Storm Supervisor"
          case a if a.contains("storm ui") => "Storm UI"
          case a if a.contains("storm") => "Storm"
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
   * pwd - Print working directory
   * @param args the given arguments
   * @return the current working directory
   */
  def printWorkingDirectory(args: String*) = {
    new File(cwd).getCanonicalPath
  }

  /**
   * Retrieves "netstat -ptln" and "ps -ef" data from a remote node
   * @param node the given remote node (e.g. "Verify")
   * @return a future containing the data
   */
  private def remoteData(node: String): Future[(Seq[String], Map[String, String])] = {
    import scala.io.Source
    import scala.sys.process._

    // asynchronously get the raw output from 'ps -ef'
    val psdataF: Future[Seq[String]] = Future {
      Source.fromString((node match {
        case "." => "ps -ef"
        case host => s"ssh -i /home/ubuntu/dev.pem ubuntu@$host ps -ef"
      }).!!).getLines().toSeq
    }

    // asynchronously get the port mapping
    val portmapF: Future[Map[String, String]] = Future {
      // get the lines of data from 'netstat'
      val netStat = Source.fromString((node match {
        case "." => "netstat -ptln"
        case host => s"ssh -i /home/ubuntu/dev.pem ubuntu@$host netstat -ptln"
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
        case (pid, seq) => (pid, seq.sortBy(_._2).reverse map (_._2) mkString ", ")
      }
    }

    // let's combine the futures
    for {
      pasdata <- psdataF
      portmap <- portmapF
    } yield (pasdata, portmap)
  }

  /**
   * "storm" command - Deploys a topology to the Storm server
   * Example: storm mytopology.jar myconfig.properties
   */
  def stormDeploy(args: String*): String = {
    import scala.sys.process._

    // deploy the topology
    s"storm jar ${args mkString " "}".!!
  }

  /**
   * "systime" command - Returns the system time as an EPOC in milliseconds
   */
  def systemTime(args: String*) = System.currentTimeMillis.toString

  /**
   * "time" command - Returns the time in the local time zone
   */
  def time(args: String*): Date = new Date()

  /**
   * "timeutc" command - Returns the time in the GMT time zone
   */
  def timeUTC(args: String*): String = {
    import java.text.SimpleDateFormat

    val fmt = new SimpleDateFormat("E MMM dd HH:mm:ss z yyyy")
    fmt.setTimeZone(TimeZone.getTimeZone("GMT"))
    fmt.format(new Date())
  }

}
