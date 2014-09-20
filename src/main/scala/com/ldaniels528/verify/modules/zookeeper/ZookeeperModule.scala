package com.ldaniels528.verify.modules.zookeeper

import java.io.PrintStream
import java.nio.ByteBuffer
import java.util.Date

import com.ldaniels528.verify.VxRuntimeContext
import com.ldaniels528.verify.command.CommandParser._
import com.ldaniels528.verify.command._
import com.ldaniels528.verify.modules._
import com.ldaniels528.verify.support.zookeeper.ZKProxy
import com.ldaniels528.verify.support.zookeeper.ZKProxy.Implicits._
import com.ldaniels528.verify.util.EndPoint
import com.ldaniels528.verify.util.VxUtils._
import com.ldaniels528.verify.vscript.VScriptRuntime.ConstantValue
import com.ldaniels528.verify.vscript.Variable

import scala.util.Try

/**
 * Apache Zookeeper Module
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class ZookeeperModule(rt: VxRuntimeContext) extends Module {
  private implicit val out: PrintStream = rt.out
  private val zk: ZKProxy = rt.zkProxy

  override def getCommands = Seq(
    Command(this, "zcd", chdir, SimpleParams(Seq("key"), Seq.empty), help = "Changes the current path/directory in ZooKeeper"),
    Command(this, "zexists", pathExists, SimpleParams(Seq("key"), Seq.empty), "Verifies the existence of a ZooKeeper key"),
    Command(this, "zget", getMessage, UnixLikeParams(Seq("key" -> true), Seq("-t" -> "type")), "Retrieves the contents of a specific Zookeeper key"),
    Command(this, "zls", listKeys, SimpleParams(Seq.empty, Seq("path")), help = "Retrieves the child nodes for a key from ZooKeeper"),
    Command(this, "zmk", mkdir, SimpleParams(Seq("key"), Seq.empty), "Creates a new ZooKeeper sub-directory (key)"),
    Command(this, "zput", publishMessage, UnixLikeParams(Seq("key" -> true, "value" -> true), Seq("-t" -> "type")), "Sets a key-value pair in ZooKeeper"),
    Command(this, "zreconnect", reconnect, SimpleParams(Seq.empty, Seq.empty), help = "Re-establishes the connection to Zookeeper"),
    Command(this, "zrm", delete, UnixLikeParams(Seq("key" -> true), flags = Seq("-r" -> "recursive")), "Removes a key-value from ZooKeeper (DESTRUCTIVE)"),
    Command(this, "zruok", ruok, SimpleParams(), help = "Checks the status of a Zookeeper instance (requires netcat)"),
    Command(this, "zsess", session, SimpleParams(), help = "Retrieves the Session ID from ZooKeeper"),
    Command(this, "zstats", stats, SimpleParams(), help = "Returns the statistics of a Zookeeper instance (requires netcat)"),
    Command(this, "ztree", tree, SimpleParams(Seq.empty, Seq("path")), help = "Retrieves Zookeeper directory structure"))

  override def getVariables: Seq[Variable] = Seq(
    Variable("zkCwd", ConstantValue(Option("/"))))

  override def moduleName = "zookeeper"

  override def prompt: String = s"${rt.remoteHost}${rt.zkCwd}"

  override def shutdown() = ()

  var prevCwd = "/"

  /**
   * "zcd" - Changes the current path/directory in ZooKeeper
   * @example zcd /
   * @example zcd ..
   * @example zcd brokers
   */
  def chdir(params: UnixLikeArgs): Either[String, Unit] = {
    // if more than 1 argument, it's an error
    if (params.args.length != 1) dieSyntax(params)

    // perform the action
    val newPath: Option[String] = params.args.headOption map {
      case "-" => prevCwd
      case "." => rt.zkCwd
      case ".." =>
        rt.zkCwd.split("[/]") match {
          case a if a.length <= 1 => "/"
          case a =>
            val aPath = a.init.mkString("/")
            if (aPath.trim.isEmpty) "/" else aPath
        }
      case path => zkKeyToPath(path)
    }

    // if argument was a dot (.) return the current path
    newPath match {
      case Some(path) =>
        prevCwd = rt.zkCwd
        rt.zkCwd = path
        Right(())
      case None => Left(rt.zkCwd)
    }
  }

  /**
   * "zrm" - Removes a key-value from ZooKeeper
   * @example zrm /consumer/GRP000a2ce
   * @example zrm -r /consumer/GRP000a2ce
   */
  def delete(params: UnixLikeArgs) {
    (params("-r") ?? params.args.headOption) map zkKeyToPath foreach { path =>
      // recursive delete?
      if (params.contains("-r")) deleteRecursively(path) else zk.delete(path)
    }
  }

  /**
   * Performs a recursive delete
   * @param path the path to delete
   */
  private def deleteRecursively(path: String): Unit = {
    zk.getChildren(path) foreach (subPath => deleteRecursively(zkKeyToPath(path, subPath)))

    out.println(s"Deleting '$path'...")
    zk.delete(path)
  }

  /**
   * "zget" - Dumps the contents of a specific Zookeeper key to the console
   * @example zget /storm/workerbeats/my-test-topology-17-1407973634
   */
  def getMessage(params: UnixLikeArgs): Option[Any] = {
    // get the key
    params.args.headOption flatMap { key =>
      // convert the key to a fully-qualified path
      val path = zkKeyToPath(key)

      // retrieve (or guess) the value's type
      val valueType = params("-t") getOrElse "bytes"

      // perform the action
      zk.read(path) map (decodeValue(_, valueType))
    }
  }

  /**
   * "zls" - Retrieves the child nodes for a key from ZooKeeper
   * @example zls topics/Shocktrade.quotes.csv/partitions/4/state
   */
  def listKeys(params: UnixLikeArgs): Seq[String] = {
    // get the argument
    val path = params.args.headOption map zkKeyToPath getOrElse rt.zkCwd

    // perform the action
    zk.getChildren(path, watch = false)
  }

  /**
   * "zmk" - Creates a new ZooKeeper sub-directory (key)
   */
  def mkdir(params: UnixLikeArgs): List[String] = {
    params.args.headOption map (key => zk.ensurePath(zkKeyToPath(key))) getOrElse Nil
  }

  /**
   * "zexists" - Returns the status of a Zookeeper key if it exists
   */
  def pathExists(params: UnixLikeArgs): Seq[String] = {
    // get the argument
    params.args.headOption map { key =>
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
    } getOrElse Nil
  }

  /**
   * "zput" - Sets a key-value pair in ZooKeeper
   * @example zput /test/data/items/1 "Hello World"
   * @example zput /test/data/items/2 1234.5
   * @example zput /test/data/items/3 12345 -t short
   * @example zput /test/data/items/4 de.ad.be.ef
   */
  def publishMessage(params: UnixLikeArgs) {
    // get the arguments
    params.args match {
      case key :: value :: Nil =>
        // convert the key to a fully-qualified path
        val path = zkKeyToPath(key)

        // retrieve (or guess) the value's type
        val valueType = params("-t") getOrElse guessValueType(value)

        // perform the action
        Try(zk delete path)
        Try(zk ensureParents path)
        zk.create(path -> encodeValue(value, valueType))

      case _ =>
        dieSyntax(params)
    }
    ()
  }

  /**
   * Re-establishes the connection to Zookeeper
   */
  def reconnect(params: UnixLikeArgs): Unit = zk.reconnect()

  /**
   * "zruok" - Checks the status of a Zookeeper instance
   * (e.g. echo ruok | nc zookeeper 2181)
   */
  def ruok(params: UnixLikeArgs): String = {
    import scala.sys.process._

    // echo ruok | nc zookeeper 2181
    val (host, port) = EndPoint(zk.remoteHost).unapply()
    ("echo ruok" #> s"nc $host $port").!!
  }

  /**
   * "zstats" - Returns the statistics of a Zookeeper instance
   * echo stat | nc zookeeper 2181
   */
  def stats(params: UnixLikeArgs): String = {
    import scala.sys.process._

    // echo stat | nc zookeeper 2181
    val (host, port) = EndPoint(zk.remoteHost).unapply()
    ("echo stat" #> s"nc $host $port").!!
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
   * "zsession" - Retrieves the Session ID from ZooKeeper
   */
  def session(params: UnixLikeArgs) = zk.getSessionId.toString

  /**
   * "ztree" - Retrieves ZooKeeper key hierarchy
   * @param params the given arguments
   */
  def tree(params: UnixLikeArgs): Seq[String] = {

    def unwind(path: String): List[String] = {
      val children = Option(zk.getChildren(path, watch = false)) getOrElse Seq.empty
      path :: (children flatMap (child => unwind(zkKeyToPath(path, child)))).toList
    }

    // get the optional path argument
    val path = params.args.headOption map zkKeyToPath getOrElse rt.zkCwd

    // perform the action
    unwind(path)
  }

  private def decodeValue(bytes: Array[Byte], valueType: String): Any = {
    valueType match {
      case "bytes" => bytes
      case "char" => ByteBuffer.wrap(bytes).getChar
      case "double" => ByteBuffer.wrap(bytes).getDouble
      case "float" => ByteBuffer.wrap(bytes).getFloat
      case "int" | "integer" => ByteBuffer.wrap(bytes).getInt
      case "json" => formatJson(new String(bytes))
      case "long" => ByteBuffer.wrap(bytes).getLong
      case "short" => ByteBuffer.wrap(bytes).getShort
      case "string" | "text" => new String(bytes)
      case _ => throw new IllegalArgumentException(s"Invalid type '$valueType'")
    }
  }

  private def encodeValue(value: String, valueType: String): Array[Byte] = {
    import java.nio.ByteBuffer.allocate

    valueType match {
      case "bytes" => parseDottedHex(value)
      case "char" => allocate(2).putChar(value.head)
      case "double" => allocate(8).putDouble(value.toDouble)
      case "float" => allocate(4).putFloat(value.toFloat)
      case "int" | "integer" => allocate(4).putInt(value.toInt)
      case "long" => allocate(8).putLong(value.toLong)
      case "short" => allocate(2).putShort(value.toShort)
      case "string" | "text" => value.getBytes
      case _ => throw new IllegalArgumentException(s"Invalid type '$valueType'")
    }
  }

  private def formatJson(value: String): String = {
    import spray.json._

    val jsonAst = value.parseJson
    jsonAst.prettyPrint
  }

  /**
   * Guesses the given value's type
   * @param value the given value
   * @return the guessed type
   */
  private def guessValueType(value: String): String = {
    value match {
      case s if s.matches( """^-?[0-9]\d*(\.\d+)?$""") => "double"
      case s if s.matches( """\d+""") => "long"
      case s if isDottedHex(s) => "bytes"
      case _ => "string"
    }
  }

}
