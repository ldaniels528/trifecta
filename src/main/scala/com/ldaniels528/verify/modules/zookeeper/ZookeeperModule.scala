package com.ldaniels528.verify.modules.zookeeper

import java.io.PrintStream
import java.nio.ByteBuffer
import java.util.Date

import com.ldaniels528.verify.VxRuntimeContext
import com.ldaniels528.verify.modules.zookeeper.ZKProxy.Implicits._
import com.ldaniels528.verify.modules.{Command, Module}
import com.ldaniels528.verify.vscript.Variable

/**
 * Zookeeper Module
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class ZookeeperModule(rt: VxRuntimeContext) extends Module {
  private implicit val out: PrintStream = rt.out
  private val zk: ZKProxy = rt.zkProxy

  override def getCommands = Seq(
    Command(this, "zcat", zcat, (Seq("key", "type"), Seq.empty), "Retrieves the value of a key from ZooKeeper"),
    Command(this, "zcd", zcd, (Seq("key"), Seq.empty), help = "Changes the current path/directory in ZooKeeper"),
    Command(this, "zexists", zexists, (Seq("key"), Seq.empty), "Verifies the existence of a ZooKeeper key"),
    Command(this, "zget", zget, (Seq("key"), Seq.empty), "Retrieves the contents of a specific Zookeeper key"),
    Command(this, "zls", zls, (Seq.empty, Seq("path")), help = "Retrieves the child nodes for a key from ZooKeeper"),
    Command(this, "zmk", zmkdir, (Seq("key"), Seq.empty), "Creates a new ZooKeeper sub-directory (key)"),
    Command(this, "zput", zput, (Seq("key", "value", "type"), Seq.empty), "Retrieves a value from ZooKeeper"),
    Command(this, "zreconnect", reconnect, (Seq.empty, Seq.empty), help = "Re-establishes the connection to Zookeeper"),
    Command(this, "zrm", delete, (Seq("key"), Seq.empty), "Removes a key-value from ZooKeeper (DESTRUCTIVE)"),
    Command(this, "zruok", ruok, help = "Checks the status of a Zookeeper instance (requires netcat)"),
    Command(this, "zsess", session, help = "Retrieves the Session ID from ZooKeeper"),
    Command(this, "zstat", stat, help = "Returns the statistics of a Zookeeper instance (requires netcat)"),
    Command(this, "ztree", tree, (Seq.empty, Seq("path")), help = "Retrieves Zookeeper directory structure"))

  override def getVariables: Seq[Variable] = Seq.empty

  override def moduleName = "zookeeper"

  override def prompt: String = s"${rt.remoteHost}${rt.zkCwd}"

  override def shutdown() = ()

  /**
   * "zget" - Dumps the contents of a specific Zookeeper key to the console
   * @example {{{ zget /storm/workerbeats/my-test-topology-17-1407973634 }}}
   */
  def zget(args: String*)(implicit out: PrintStream) {
    // get the Zookeeper key
    val key = args.head

    // convert the key to a fully-qualified path
    val path = zkKeyToPath(key)

    // perform the action
    val columns = rt.columns
    val byteWidth = columns * 3
    zk.read(path) foreach { bytes =>
      val length = 1 + Math.log10(bytes.length).toInt
      var offset = 0
      val myFormat = s"[%0${length}d] %-${byteWidth}s| %-${columns}s"
      bytes.sliding(columns, columns) foreach { bytes =>
        out.println(myFormat.format(offset, asHexString(bytes), asChars(bytes)))
        offset += columns
      }
    }
  }

  /**
   * "zruok" - Checks the status of a Zookeeper instance
   * (e.g. echo ruok | nc zookeeper 2181)
   */
  def ruok(args: String*): String = {
    import scala.sys.process._

    // echo ruok | nc zookeeper 2181
    val (host, port) = rt.zkEndPoint()
    ("echo ruok" #> s"nc $host $port").!!
  }

  /**
   * "zstat" - Returns the statistics of a Zookeeper instance
   * echo stat | nc zookeeper 2181
   */
  def stat(args: String*): String = {
    import scala.sys.process._

    // echo stat | nc zookeeper 2181
    val (host, port) = rt.zkEndPoint()
    ("echo stat" #> s"nc $host $port").!!
  }

  /**
   * "zcd" - Changes the current path/directory in ZooKeeper
   * @example {{{ zcd brokers }}}
   */
  def zcd(args: String*): String = {
    // get the argument
    val key = args.head

    // perform the action
    rt.zkCwd = key match {
      case s if s == ".." =>
        rt.zkCwd.split("[/]") match {
          case a if a.length <= 1 => "/"
          case a =>
            val newPath = a.init.mkString("/")
            if (newPath.trim.length == 0) "/" else newPath
        }
      case s => zkKeyToPath(s)
    }
    rt.zkCwd
  }

  /**
   * "zls" - Retrieves the child nodes for a key from ZooKeeper
   */
  def zls(args: String*): Seq[String] = {
    // get the argument
    val path = if (args.nonEmpty) zkKeyToPath(args.head) else rt.zkCwd

    // perform the action
    zk.getChildren(path, watch = false)
  }

  /**
   * "zrm" - Removes a key-value from ZooKeeper
   * @example {{{ zrm brokers }}}
   */
  def delete(args: String*) {
    // get the argument
    val key = args.head

    // convert the key to a fully-qualified path
    val path = zkKeyToPath(key)

    // perform the action
    zk.exists(path) foreach (zk.delete(path, _))
  }

  /**
   * "zexists" - Returns the status of a Zookeeper key if it exists
   */
  def zexists(args: String*): Seq[String] = {
    // get the argument
    val key = args.head

    // convert the key to a fully-qualified path
    val path = zkKeyToPath(key)

    // perform the action
    zk.exists(path) match {
      case Some(stat) =>
        Seq(
          s"Aversion: ${stat.getAversion}",
          s"version: ${stat.getVersion}",
          s"data length: ${stat.getDataLength}",
          s"children: ${stat.getNumChildren}",
          s"change time: ${new Date(stat.getCtime)}")
      case None =>
        Seq.empty
    }
  }

  /**
   * "zcat" - Outputs the value of a key from ZooKeeper
   */
  def zcat(args: String*): Option[String] = {
    // get the arguments
    val Seq(key, typeName, _*) = args

    // convert the key to a fully-qualified path
    val path = zkKeyToPath(key)

    // perform the action
    zk.read(path) map { bytes =>
      fromBytes(bytes, typeName)
    }
  }

  private def zkKeyToPath(parent: String, child: String): String = {
    val parentWithSlash = if (parent.endsWith("/")) parent else parent + "/"
    parentWithSlash + child
  }

  private def zkKeyToPath(key: String): String = {
    key match {
      case s if s.startsWith("/") => key
      case s => (if (rt.zkCwd.endsWith("/")) rt.zkCwd else rt.zkCwd + "/") + s
    }
  }

  /**
   * "zmk" - Creates a new ZooKeeper sub-directory (key)
   */
  def zmkdir(args: String*) = {
    // get the arguments
    val Seq(key, _*) = args

    // perform the action
    zk.ensurePath(zkKeyToPath(key))
  }

  /**
   * "zput" - Retrieves a value from ZooKeeper
   */
  def zput(args: String*) = {
    // get the arguments
    val Seq(key, value, typeName, _*) = args

    // convert the key to a fully-qualified path
    val path = zkKeyToPath(key)

    // perform the action
    zk.exists(path) foreach (zk.delete(path, _))
    (zk ensureParents path).create(path -> toBytes(value, typeName))
  }

  /**
   * Re-establishes the connection to Zookeeper
   */
  def reconnect(args: String*) = zk.reconnect()

  /**
   * "zsession" - Retrieves the Session ID from ZooKeeper
   */
  def session(args: String*) = zk.getSessionId.toString

  /**
   * "ztree" - Retrieves ZooKeeper key hierarchy
   * @param args the given arguments
   * @return
   */
  def tree(args: String*): Seq[String] = {

    def recurse(path: String): List[String] = {
      val children = Option(zk.getChildren(path, watch = false)) getOrElse Seq.empty
      path :: (children flatMap (child => recurse(zkKeyToPath(path, child)))).toList
    }

    // get the optional path argument
    val path = if (args.nonEmpty) zkKeyToPath(args.head) else rt.zkCwd

    // perform the action
    recurse(path)
  }

  private def fromBytes(bytes: Array[Byte], typeName: String): String = {
    typeName match {
      case "hex" => bytes map (b => "%02x".format(b)) mkString "."
      case "int" => ByteBuffer.wrap(bytes).getInt.toString
      case "long" => ByteBuffer.wrap(bytes).getLong.toString
      case "dec" | "double" => ByteBuffer.wrap(bytes).getDouble.toString
      case "string" | "text" => new String(bytes)
      case _ => throw new IllegalArgumentException(s"Invalid type '$typeName'")
    }
  }

  private def toBytes(value: String, typeName: String): Array[Byte] = {
    import java.nio.ByteBuffer.allocate

    typeName match {
      case "hex" => value.getBytes
      case "int" => allocate(4).putInt(value.toInt)
      case "long" => allocate(8).putLong(value.toLong)
      case "dec" | "double" => allocate(8).putDouble(value.toDouble)
      case "string" | "text" => value.getBytes
      case _ => throw new IllegalArgumentException(s"Invalid type '$typeName'")
    }
  }

}
