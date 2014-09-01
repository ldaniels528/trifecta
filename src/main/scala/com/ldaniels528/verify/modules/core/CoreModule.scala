package com.ldaniels528.verify.modules.core

import java.io.{File, PrintStream}
import java.util.{Date, TimeZone}

import com.ldaniels528.verify.modules.ModuleManager.ModuleVariable
import com.ldaniels528.verify.modules.{Command, Module}
import com.ldaniels528.verify.util.VxUtils._
import com.ldaniels528.verify.vscript.VScriptRuntime.ConstantValue
import com.ldaniels528.verify.vscript.{Scope, Variable}
import com.ldaniels528.verify.{SessionManagement, VerifyShell, VxRuntimeContext}
import org.apache.commons.io.IOUtils

import scala.concurrent.ExecutionContext.Implicits._
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.language.postfixOps
import scala.util.Properties

/**
 * Core Module
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class CoreModule(rt: VxRuntimeContext) extends Module {
  private implicit val scope: Scope = rt.scope
  private implicit val out: PrintStream = rt.out

  // define the process parsing regular expression
  private val PID_MacOS_r = "^\\s*(\\d+)\\s*(\\d+)\\s*(\\d+)\\s*(\\S+)\\s*(\\S+)\\s*(\\S+)\\s*(\\S+)\\s*(.*)".r
  private val PID_Linux_r = "^\\s*(\\S+)\\s*(\\d+)\\s*(\\d+)\\s*(\\d+)\\s*(\\S+)\\s*(\\S+)\\s*(\\S+)\\s*(\\S+)\\s*(.*)".r
  private val NET_STAT_r = "^\\s*(\\S+)\\s*(\\S+)\\s*(\\S+)\\s*(\\S+)\\s*(\\S+)\\s*(\\S+)\\s*(.*)".r

  override def moduleName = "core"

  override def getCommands: Seq[Command] = Seq(
    Command(this, "!", executeHistory, (Seq("index"), Seq.empty), help = "Executes a previously issued command"),
    Command(this, "?", help, (Seq.empty, Seq("search-term")), help = "Provides the list of available commands"),
    Command(this, "autoswitch", autoSwitch, (Seq.empty, Seq("state")), help = "Automatically switches to the module of the most recently executed command"),
    Command(this, "cat", cat, (Seq("file"), Seq.empty), help = "Dumps the contents of the given file", promptAware = true),
    Command(this, "cd", changeDir, (Seq("path"), Seq.empty), help = "Changes the local file system path/directory", promptAware = true),
    Command(this, "charset", charSet, (Seq.empty, Seq("encoding")), help = "Retrieves or sets the character encoding"),
    Command(this, "class", inspectClass, (Seq.empty, Seq("action")), help = "Inspects a class using reflection"),
    Command(this, "columns", columnWidthGetOrSet, (Seq.empty, Seq("columnWidth")), help = "Retrieves or sets the column width for message output"),
    Command(this, "debug", debug, (Seq.empty, Seq("enabled")), help = "Switches debugging on/off"),
    Command(this, "exit", exit, help = "Exits the shell"),
    Command(this, "help", help, help = "Provides the list of available commands"),
    Command(this, "history", listHistory, help = "Returns a list of previously issued commands"),
    Command(this, "hostname", hostname, help = "Returns the name of the host system"),
    Command(this, "ls", listFiles, (Seq.empty, Seq("path")), help = "Retrieves the files from the current directory", promptAware = true),
    Command(this, "modules", listModules, help = "Returns a list of configured modules"),
    Command(this, "pkill", processKill, (Seq("pid0"), Seq("pid1", "pid2", "pid3", "pid4", "pid5", "pid6")), help = "Terminates specific running processes"),
    Command(this, "ps", processList, (Seq.empty, Seq("node", "timeout")), help = "Display a list of \"configured\" running processes (EXPERIMENTAL)"),
    Command(this, "pwd", printWorkingDirectory, (Seq.empty, Seq.empty), help = "Display current working directory"),
    Command(this, "resource", findResource, (Seq("resource-name"), Seq.empty), help = "Inspects the classpath for the given resource"),
    Command(this, "runjava", executeJavaApp, (Seq("jarFile", "className"), (1 to 10).map(n => s"arg$n")), help = "Executes a Java class' main method"),
    Command(this, "scope", listScope, help = "Returns the contents of the current scope"),
    Command(this, "syntax", syntax, (Seq("command"), Seq.empty), help = "Returns the syntax/usage for a given command"),
    Command(this, "systime", systemTime, help = "Returns the system time as an EPOC in milliseconds"),
    Command(this, "time", time, help = "Returns the system time"),
    Command(this, "timeutc", timeUTC, help = "Returns the system time in UTC"),
    Command(this, "use", useModule, (Seq("module"), Seq.empty), help = "Switches the active module"),
    Command(this, "version", version, help = "Returns the Verify application version"),
    Command(this, "wget", httpGet, Seq("url") -> Seq.empty, help = "Retrieves remote content via HTTP"))

  override def getVariables: Seq[Variable] = Seq(
    Variable("autoSwitching", ConstantValue(Option(true))),
    Variable("columns", ConstantValue(Option(25))),
    Variable("cwd", ConstantValue(Option(new File(".").getCanonicalPath))),
    Variable("debugOn", ConstantValue(Option(false))),
    Variable("encoding", ConstantValue(Option("UTF8")))
  )

  override def prompt: String = cwd

  override def shutdown() = ()

  // load the commands from the modules
  private def commandSet: Map[String, Command] = rt.moduleManager.commandSet

  /**
   * Retrieves the current working directory
   */
  def cwd: String = scope.getValue[String]("cwd") getOrElse "."

  /**
   * Sets the current working directory
   * @param path the path to set
   */
  def cwd_=(path: String) = scope.setValue("cwd", Option(path))

  /**
   * Automatically switches to the module of the most recently executed command
   * @example {{{ autoswitch true }}}
   */
  def autoSwitch(args: String*): String = {
    args.headOption map (_.toBoolean) foreach (rt.autoSwitching = _)
    s"auto switching is ${if (rt.autoSwitching) "On" else "Off"}"
  }

  /**
   * Displays the contents of the given file
   * @example {{{ cat avro/schema1.avsc }}}
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
            val newPath = a.init.mkString("/")
            if (newPath.trim.length == 0) "/" else newPath
        }
      case s => setupPath(s)
    }
    cwd
  }

  /**
   * Retrieves or sets the character encoding
   * Example: charset UTF-8
   * @param args the given arguments
   */
  def charSet(args: String*) = {
    args.headOption match {
      case Some(newEncoding) => rt.encoding = newEncoding
      case None => rt.encoding
    }
  }

  /**
   * "columns" - Retrieves or sets the column width for message output
   * @example {{{ columns 30 }}}
   */
  def columnWidthGetOrSet(args: String*): Any = {
    args.headOption match {
      case Some(arg) => rt.columns = parseInt("columnWidth", arg)
      case None => rt.columns
    }
  }

  /**
   * Toggles the current debug state
   * @param args the given command line arguments
   * @return the current state ("On" or "Off")
   */
  def debug(args: String*): String = {
    if (args.isEmpty) rt.debugOn = !rt.debugOn else rt.debugOn = args.head.toBoolean
    s"debugging is ${if (rt.debugOn) "On" else "Off"}"
  }

  /**
   * Executes a Java class' main method
   * @example {{{ runjava myJarFile.jar com.shocktrade.test.Tester }}}
   * @return the program's output
   */
  def executeJavaApp(args: String*): Iterator[String] = {
    val Seq(jarPath, className, _*) = args
    runJava(jarPath, className, args.drop(2): _*)
  }

  /**
   * Inspects the classpath for the given resource by name
   * Example: resource org/apache/http/message/BasicLineFormatter.class
   */
  def findResource(args: String*): String = {
    // get the class name (with slashes)
    val path = args.head
    val index = path.lastIndexOf('.')
    val resourceName = path.substring(0, index).replace('.', '/') + path.substring(index)

    // determine the resource
    val classLoader = VerifyShell.getClass.getClassLoader
    val resource = classLoader.getResource(resourceName)
    String.valueOf(resource)
  }

  /**
   * "help" command - Provides the list of available commands
   */
  def help(args: String*): Seq[CommandItem] = {
    commandSet.toSeq filter {
      case (nameA, _) => args.isEmpty || nameA.startsWith(args.head)
    } sortBy (_._1) map {
      case (nameB, cmdB) => CommandItem(nameB, cmdB.module.moduleName, cmdB.help)
    }
  }

  /**
   * "hostname" command - Returns the name of the current host
   */
  def hostname(args: String*): String = {
    java.net.InetAddress.getLocalHost.getHostName
  }

  /**
   * "wget" command - Retrieves remote content via HTTP
   * @example {{{ wget http://www.example.com/ }}}
   */
  def httpGet(args: String*): Array[Byte] = {
    import java.io.ByteArrayOutputStream
    import java.net._

    // get the URL string
    val urlString = args.head

    // download the content
    new URL(urlString).openConnection().asInstanceOf[HttpURLConnection] use { conn =>
      conn.getInputStream use { in =>
        val out = new ByteArrayOutputStream(1024)
        IOUtils.copy(in, out)
        out.toByteArray
      }
    }
  }

  /**
   * Inspects a class using reflection
   * Example: class org.apache.commons.io.IOUtils -m
   */
  def inspectClass(args: String*): Seq[String] = {
    val className = extract(args, 0).getOrElse(getClass.getName).replace('/', '.')
    val action = extract(args, 1) getOrElse "-m"
    val beanClass = Class.forName(className)

    action match {
      case "-m" => beanClass.getDeclaredMethods map (_.toString)
      case "-f" => beanClass.getDeclaredFields map (_.toString)
      case _ => beanClass.getDeclaredMethods map (_.toString)
    }
  }

  /**
   * "!" command - History execution command. This command can either executed a
   * previously executed command by its unique identifier, or list (!?) all previously
   * executed commands.
   * Example 1: !123
   * Example 2: !?
   */
  def executeHistory(args: String*)(implicit out: PrintStream) = {
    for {
      index <- args.headOption
      command <- index match {
        case s if s == "?" => Some("history")
        case s if s == "!" => SessionManagement.history.last
        case s if s.matches("\\d+") => SessionManagement.history(index.toInt - 1)
        case s =>
          throw new IllegalArgumentException(s"Unrecognized symbol '$s'")
      }
    } {
      out.println(s">> $command")
      val result = rt.interpret(command)
      rt.handleResult(result)
    }
  }

  /**
   * "exit" command - Exits the shell
   */
  def exit(args: String*) = {
    rt.alive = false
    SessionManagement.history.store(rt.historyFile)
  }

  /**
   * "ls" - Retrieves the files from the current directory
   */
  def listFiles(args: String*): Seq[String] = {
    // get the argument
    val path = if (args.nonEmpty) setupPath(args.head) else cwd

    // perform the action
    new File(path).list map { file =>
      if (file.startsWith(path)) file.substring(path.length) else file
    }
  }

  def listHistory(args: String*): Seq[HistoryItem] = {
    val lines = SessionManagement.history.getLines
    ((1 to lines.size) zip lines) map {
      case (itemNo, command) => HistoryItem(itemNo, command)
    }
  }

  /**
   * "modules" command - Returns the list of modules
   * Example: modules
   * @return the list of modules
   */
  def listModules(args: String*): Seq[ModuleItem] = {
    val activeModule = rt.moduleManager.activeModule
    rt.moduleManager.modules.map(m =>
      ModuleItem(m.moduleName, m.getClass.getName, loaded = true, activeModule.exists(_.moduleName == m.moduleName)))
  }

  def listScope(args: String*): Seq[ScopeItem] = {
    // get the variables (filter out duplicates)
    val varsA: Seq[ModuleVariable] = rt.moduleManager.variableSet
    val varsB: Seq[Variable] = {
      val names: Set[String] = varsA.map(_.variable.name).toSet
      scope.getVariables filterNot (v => names.contains(v.name))
    }

    // build the list of functions, variables, etc.
    (varsA map (v => ScopeItem(v.variable.name, v.moduleName, "variable", v.variable.eval))) ++
      (varsB map (v => ScopeItem(v.name, "", "variable", v.eval))) ++
      (scope.getFunctions map (f => ScopeItem(f.name, "", "function"))) sortBy (_.name)
  }

  case class ScopeItem(name: String, module: String, `type`: String, value: Option[_] = None)

  /**
   * "ps" command - Display a list of "configured" running processes
   */
  def processList(args: String*)(implicit out: PrintStream): Seq[String] = {
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
        case "." if Properties.isMac => "netstat -gilns"
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

  def syntax(args: String*): Seq[String] = {
    val commandName = args.head

    rt.moduleManager.findCommandByName(commandName) match {
      case Some(command) => Seq(s"Description: ${command.help}", s"Usage: ${command.prototype}")
      case None =>
        throw new IllegalStateException( s"Command '$commandName' not found")
    }
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

  /**
   * "use" command - Switches the active module
   * Example: use kafka
   */
  def useModule(args: String*) = {
    val moduleName = args.head
    rt.moduleManager.findModuleByName(moduleName) match {
      case Some(module) => rt.moduleManager.activeModule = module
      case None =>
        throw new IllegalArgumentException(s"Module '$moduleName' not found")
    }
  }

  private def setupPath(key: String): String = {
    key match {
      case s if s.startsWith("/") => key
      case s => (if (cwd.endsWith("/")) cwd else cwd + "/") + s
    }
  }

  /**
   * "version" - Returns the application version
   * @return the application version
   */
  def version(args: String*): String = VerifyShell.VERSION

  case class CommandItem(command: String, module: String, description: String)

  case class HistoryItem(uid: Int, command: String)

  case class ModuleItem(name: String, className: String, loaded: Boolean, active: Boolean)

}


