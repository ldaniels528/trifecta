package com.ldaniels528.trifecta.modules.zookeeper

import java.io.PrintStream
import java.nio.ByteBuffer
import java.util.Date

import com.ldaniels528.trifecta.command.CommandParser._
import com.ldaniels528.trifecta.command._
import com.ldaniels528.trifecta.modules._
import com.ldaniels528.trifecta.support.io.InputSource
import com.ldaniels528.trifecta.support.zookeeper.ZKProxy
import com.ldaniels528.trifecta.support.zookeeper.ZKProxy.Implicits._
import com.ldaniels528.trifecta.util.EndPoint
import com.ldaniels528.trifecta.util.TxUtils._
import com.ldaniels528.trifecta.vscript.VScriptRuntime.ConstantValue
import com.ldaniels528.trifecta.vscript.Variable
import com.ldaniels528.trifecta.{TxConfig, TxRuntimeContext}
import net.liftweb.json._

import scala.util.Try

/**
 * Apache Zookeeper Module
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class ZookeeperModule(config: TxConfig) extends Module {
  private var zkProxy_? : Option[ZKProxy] = None
  private val formatTypes = Seq("bytes", "char", "double", "float", "int", "json", "long", "short", "string")
  private val out: PrintStream = config.out

  override def getCommands(implicit rt: TxRuntimeContext) = Seq(
    Command(this, "zcd", chdir, UnixLikeParams(Seq("key" -> true)), help = "Changes the current path/directory in ZooKeeper"),
    Command(this, "zconnect", connect, UnixLikeParams(Seq("host" -> false, "port" -> false)), help = "Establishes a connection to Zookeeper"),
    Command(this, "zexists", pathExists, UnixLikeParams(Seq("key" -> true)), "Verifies the existence of a ZooKeeper key"),
    Command(this, "zget", getData, UnixLikeParams(Seq("key" -> true), Seq("-t" -> "type")), "Retrieves the contents of a specific Zookeeper key"),
    Command(this, "zls", listKeys, UnixLikeParams(Seq("path" -> false)), help = "Retrieves the child nodes for a key from ZooKeeper"),
    Command(this, "zmk", mkdir, UnixLikeParams(Seq("key" -> true)), "Creates a new ZooKeeper sub-directory (key)"),
    Command(this, "zput", publishMessage, UnixLikeParams(Seq("key" -> true, "value" -> true), Seq("-t" -> "type")), "Sets a key-value pair in ZooKeeper"),
    Command(this, "zrm", delete, UnixLikeParams(Seq("key" -> true), flags = Seq("-r" -> "recursive")), "Removes a key-value from ZooKeeper (DESTRUCTIVE)"),
    Command(this, "zruok", ruok, UnixLikeParams(), help = "Checks the status of a Zookeeper instance (requires netcat)"),
    Command(this, "zstats", stats, UnixLikeParams(), help = "Returns the statistics of a Zookeeper instance (requires netcat)"),
    Command(this, "ztree", tree, UnixLikeParams(Seq("path" -> false)), help = "Retrieves Zookeeper directory structure"))

  /**
   * Returns a Zookeeper input source
   * @param url the given input URL (e.g. "zk:/messages/cache001")
   * @return the option of a Zookeeper input source
   */
  override def getInputHandler(url: String): Option[InputSource] = None

  /**
   * Returns a Zookeeper output source
   * @param url the given output URL (e.g. "zk:/messages/cache001")
   * @return the option of a Zookeeper output source
   */
  override def getOutputHandler(url: String): Option[ZookeeperOutputSource] = {
    url.extractProperty("zk:") map (new ZookeeperOutputSource(zk, _))
  }

  override def getVariables: Seq[Variable] = Seq(
    Variable("zkPrevCwd", ConstantValue(Option("/"))),
    Variable("zkCwd", ConstantValue(Option("/"))))

  override def moduleName = "zookeeper"

  override def moduleLabel = "zk"

  override def prompt: String = zkProxy_? map(zk => s"${zk.host}:${zk.port}$zkCwd") getOrElse zkCwd

  override def shutdown() = zkProxy_?.foreach(_.close())

  override def supportedPrefixes: Seq[String] = Seq("zk")

  /**
   * Returns the ZooKeeper previous working directory
   * @return the previous working directory
   */
  def prevCwd: String = config.getOrElse("zkPrevCwd", "/")

  /**
   * Sets the ZooKeeper previous working directory
   * @param path the path to set
   */
  def prevCwd_=(path: String) = config.set("zkPrevCwd", path)

  /**
   * Returns the ZooKeeper current working directory
   * @return the current working directory
   */
  def zkCwd: String = config.getOrElse("zkCwd", "/")

  /**
   * Sets the ZooKeeper current working directory
   * @param path the path to set
   */
  def zkCwd_=(path: String) = config.set("zkCwd", path)

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
      case "." => zkCwd
      case ".." =>
        zkCwd.split("[/]") match {
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
        if (path != zkCwd) {
          prevCwd = zkCwd
          zkCwd = path
        }
        Right(())
      case None => Left(zkCwd)
    }
  }

  /**
   * Establishes a connection to Zookeeper
   * @example zconnect
   * @example zconnect localhost
   * @example zconnect localhost 2181
   */
  def connect(params: UnixLikeArgs): Unit = {
    // determine the requested end-point
    val endPoint = params.args match {
      case Nil => EndPoint(config.zooKeeperConnect)
      case path :: Nil => EndPoint(path, 2181)
      case path :: port :: Nil => EndPoint(path, parseInt("port", port))
      case _ => dieSyntax(params)
    }

    // connect to the remote peer
    zkProxy_?.foreach(_.close())
    zkProxy_? = Option(ZKProxy(endPoint))
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
  private def deleteRecursively(path: String) {
    zk.getChildren(path) foreach (subPath => deleteRecursively(zkKeyToPath(path, subPath)))

    out.println(s"Deleting '$path'...")
    zk.delete(path)
  }

  /**
   * "zget" - Dumps the contents of a specific Zookeeper key to the console
   * @example zget /storm/workerbeats/my-test-topology-17-1407973634
   * @example zget /storm/workerbeats/my-test-topology-17-1407973634 -t json
   */
  def getData(params: UnixLikeArgs): Option[Any] = {
    // get the key
    params.args.headOption flatMap { key =>
      // convert the key to a fully-qualified path
      val path = zkKeyToPath(key)

      // retrieve (or guess) the value's format
      val valueType = params("-t") getOrElse "bytes"

      // perform the action
      zk.read(path) map (decodeValue(_, valueType))
    }
  }

  /**
   * "zls" - Retrieves the child nodes for a key from ZooKeeper
   * @example zls topics/Shocktrade.quotes.csv/partitions/4/state
   */
  def listKeys(params: UnixLikeArgs): Seq[ZkItem] = {
    // get the argument
    val path = params.args.headOption map zkKeyToPath getOrElse zkCwd

    // perform the action
    zk.getChildren(path, watch = false) map { subPath =>
      ZkItem(subPath, zk.getCreationTime(zkKeyToPath(path, subPath)).map(new Date(_)))
    }
  }

  case class ZkItem(path: String, creationTime: Option[Date])

  /**
   * "zmk" - Creates a new ZooKeeper sub-directory (key)
   * @example zmk /path/to/data
   */
  def mkdir(params: UnixLikeArgs): List[String] = {
    params.args.headOption map (key => zk.ensurePath(zkKeyToPath(key))) getOrElse Nil
  }

  /**
   * "zexists" - Returns the status of a Zookeeper key if it exists
   * @example zexists /path/to/data
   */
  def pathExists(params: UnixLikeArgs): Seq[String] = {
    // get the argument
    params.args.headOption map { key =>
      // convert the key to a fully-qualified path
      val path = zkKeyToPath(key)

      // perform the action
      zk.exists_?(path) match {
        case Some(stat) =>
          Seq(
            s"Aversion: ${stat.getAversion}",
            s"version: ${stat.getVersion}",
            s"data length: ${stat.getDataLength}",
            s"children: ${stat.getNumChildren}",
            s"change time: ${new Date(stat.getCtime)}")
        case None =>
          Nil
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

  /**
   * "ztree" - Retrieves ZooKeeper key hierarchy
   * @param params the given arguments
   */
  def tree(params: UnixLikeArgs): Seq[ZkItem] = {

    def unwind(path: String): List[String] = {
      val children = Option(zk.getChildren(path, watch = false)) getOrElse Nil
      path :: (children flatMap (child => unwind(zkKeyToPath(path, child)))).toList
    }

    // get the optional path argument
    val path = params.args.headOption map zkKeyToPath getOrElse zkCwd

    // perform the action
    val paths = unwind(path)
    paths.map(subPath => ZkItem(subPath, zk.getCreationTime(subPath).map(new Date(_))))
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
      case _ => throw new IllegalArgumentException(s"Invalid type format '$valueType'. Acceptable values are: ${formatTypes mkString ", "}")
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

  private def formatJson(value: String): String = pretty(render(parse(value)))

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

  private def zkKeyToPath(key: String): String = {
    key match {
      case s if s.startsWith("/") => key
      case s => (if (zkCwd.endsWith("/")) zkCwd else zkCwd + "/") + s
    }
  }

  private def zkKeyToPath(parent: String, child: String): String = {
    val parentWithSlash = if (parent.endsWith("/")) parent else parent + "/"
    parentWithSlash + child
  }

  /**
   * Returns the connected Zookeeper Proxy
   * @return the connected Zookeeper Proxy
   */
  private implicit def zk: ZKProxy = {
    zkProxy_? match {
      case Some(zk) => zk
      case None =>
        val zk = ZKProxy(EndPoint(config.zooKeeperConnect))
        zkProxy_? = Option(zk)
        zk
    }
  }

}
