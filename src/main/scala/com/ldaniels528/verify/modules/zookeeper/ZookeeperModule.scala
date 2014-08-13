package com.ldaniels528.verify.modules.zookeeper

import java.io.PrintStream
import java.nio.ByteBuffer
import java.util.Date

import com.ldaniels528.verify.VerifyShellRuntime
import com.ldaniels528.verify.modules.Module
import com.ldaniels528.verify.modules.Module.Command
import com.ldaniels528.verify.modules.zookeeper.ZKProxy.Implicits._

/**
 * Zookeeper Module
 * @author lawrence.daniels@gmail.com
 */
class ZookeeperModule(rt: VerifyShellRuntime, out: PrintStream) extends Module {
  // create the ZooKeeper proxy
  private val zk = rt.zkProxy

  // ZooKeeper current working directory
  private var zkcwd = "/"

  val name = "zookeeper"

  override def prompt: String = zkcwd

  val getCommands = Seq(
    Command(this, "zcd", zkChangeDir, (Seq("key"), Seq.empty), help = "Changes the current path/directory in ZooKeeper"),
    Command(this, "zexists", zkExists, (Seq("key"), Seq.empty), "Removes a key-value from ZooKeeper"),
    Command(this, "zget", zkGet, (Seq("key", "type"), Seq.empty), "Sets a key-value in ZooKeeper"),
    Command(this, "zls", zkList, (Seq.empty, Seq("path")), help = "Retrieves the child nodes for a key from ZooKeeper"),
    Command(this, "zmk", zkMkdir, (Seq("key"), Seq.empty), "Creates a new ZooKeeper sub-directory (key)"),
    Command(this, "zput", zkPut, (Seq("key", "value", "type"), Seq.empty), "Retrieves a value from ZooKeeper"),
    Command(this, "zreconnect", zReconnect, (Seq.empty, Seq.empty), help = "Re-establishes the connection to Zookeeper"),
    Command(this, "zrm", zkDelete, (Seq("key"), Seq.empty), "Removes a key-value from ZooKeeper"),
    Command(this, "zruok", zkRuok, help = "Checks the status of a Zookeeper instance"),
    Command(this, "zsess", zkSession, help = "Retrieves the Session ID from ZooKeeper"),
    Command(this, "zstat", zkStat, help = "Returns the statistics of a Zookeeper instance"),
    Command(this, "ztree", zkTree, (Seq.empty, Seq("path")), help = "Retrieves Zookeeper directory structure"))

  override def shutdown() = ()

  /**
   * "zruok" - Checks the status of a Zookeeper instance
   * (e.g. echo ruok | nc zookeeper 2181)
   */
  def zkRuok(args: String*): String = {
    import scala.sys.process._

    // echo ruok | nc zookeeper 2181
    ("echo ruok" #> s"nc ${rt.zkEndPoint.host} ${rt.zkEndPoint.port}").!!
  }

  /**
   * "zstat" - Returns the statistics of a Zookeeper instance
   * echo stat | nc zookeeper 2181
   */
  def zkStat(args: String*): String = {
    import scala.sys.process._

    // echo stat | nc zookeeper 2181
    ("echo stat" #> s"nc ${rt.zkEndPoint.host} ${rt.zkEndPoint.port}").!!
  }

  /**
   * "zcd" - Changes the current path/directory in ZooKeeper
   */
  def zkChangeDir(args: String*): String = {
    // get the argument
    val key = args.head

    // perform the action
    zkcwd = key match {
      case s if s == ".." =>
        zkcwd.split("[/]") match {
          case a if a.length <= 1 => "/"
          case a =>
            val newpath = a.init.mkString("/")
            if (newpath.trim.length == 0) "/" else newpath
        }
      case s => zkKeyToPath(s)
    }
    zkcwd
  }

  /**
   * "zls" - Retrieves the child nodes for a key from ZooKeeper
   */
  def zkList(args: String*): Seq[String] = {
    // get the argument
    val path = if (args.nonEmpty) zkKeyToPath(args.head) else zkcwd

    // perform the action
    zk.getChildren(path, watch = false)
  }

  /**
   * "zrm" - Removes a key-value from ZooKeeper
   */
  def zkDelete(args: String*) {
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
  def zkExists(args: String*): Seq[String] = {
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
   * "zget" - Sets a key-value in ZooKeeper
   */
  def zkGet(args: String*): Option[String] = {
    // get the arguments
    val Seq(key, typeName, _*) = args

    // convert the key to a fully-qualified path
    val path = zkKeyToPath(key)

    // perform the action
    zk.exists(path) map (zk.read(path, _)) match {
      case Some(bytes) => Some(fromBytes(bytes, typeName))
      case None => None
    }
  }

  private def zkKeyToPath(parent: String, child: String): String = {
    val parentWithSlash = if (parent.endsWith("/")) parent else parent + "/"
    parentWithSlash + child
  }

  private def zkKeyToPath(key: String): String = {
    key match {
      case s if s.startsWith("/") => key
      case s => (if (zkcwd.endsWith("/")) zkcwd else zkcwd + "/") + s
    }
  }

  /**
   * "zmk" - Creates a new ZooKeeper sub-directory (key)
   */
  def zkMkdir(args: String*) = {
    // get the arguments
    val Seq(key, _*) = args

    // perform the action
    zk.ensurePath(zkKeyToPath(key))
  }

  /**
   * "zput" - Retrieves a value from ZooKeeper
   */
  def zkPut(args: String*) = {
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
  def zReconnect(args: String*) = zk.reconnect()

  /**
   * "zsession" - Retrieves the Session ID from ZooKeeper
   */
  def zkSession(args: String*) = zk.getSessionId.toString

  /**
   * "ztree" - Retrieves ZooKeeper key hierarchy
   * @param args the given arguments
   * @return
   */
  def zkTree(args: String*): Seq[String] = {

    def recurse(path: String): List[String] = {
      val children = Option(zk.getChildren(path, watch = false)) getOrElse Seq.empty
      path :: (children flatMap (child => recurse(zkKeyToPath(path, child)))).toList
    }

    // get the optional path argument
    val path = if (args.nonEmpty) zkKeyToPath(args.head) else zkcwd

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
