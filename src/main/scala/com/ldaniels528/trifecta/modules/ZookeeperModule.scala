package com.ldaniels528.trifecta.modules

import java.util.Date

import com.ldaniels528.trifecta.command._
import com.ldaniels528.trifecta.command.parser.CommandParser._
import com.ldaniels528.trifecta.io.InputSource
import com.ldaniels528.trifecta.io.kafka.KafkaSandbox
import com.ldaniels528.trifecta.io.zookeeper.ZkSupportHelper._
import com.ldaniels528.trifecta.io.zookeeper.{ZKProxy, ZookeeperOutputSource}
import com.ldaniels528.commons.helpers.StringHelper._
import com.ldaniels528.trifecta.{TxConfig, TxRuntimeContext}

import scala.util.Try

/**
 * Apache Zookeeper Module
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class ZookeeperModule(config: TxConfig) extends Module {
  private var zkProxy_? : Option[ZKProxy] = None

  override def getCommands(implicit rt: TxRuntimeContext) = Seq(
    Command(this, "zcd", chdir, UnixLikeParams(Seq("key" -> true)), help = "Changes the current path/directory in ZooKeeper"),
    Command(this, "zconnect", connect, UnixLikeParams(Seq("host" -> false, "port" -> false)), help = "Establishes a connection to Zookeeper"),
    Command(this, "zdel", delete, UnixLikeParams(Seq("key" -> true)), "Deletes a key-value from ZooKeeper (DESTRUCTIVE)"),
    Command(this, "zdelall", deleteRecursively, UnixLikeParams(Seq("key" -> true)), "Recursively deletes all children for a given key in ZooKeeper (DESTRUCTIVE)"),
    Command(this, "zexists", pathExists, UnixLikeParams(Seq("key" -> true)), "Verifies the existence of a ZooKeeper key"),
    Command(this, "zget", getData, UnixLikeParams(Seq("key" -> true), Seq("-f" -> "format")), "Retrieves the contents of a specific Zookeeper key"),
    Command(this, "zls", listKeys, UnixLikeParams(Seq("path" -> false)), help = "Retrieves the child nodes for a key from ZooKeeper"),
    Command(this, "zmk", mkdir, UnixLikeParams(Seq("key" -> true)), "Creates a new ZooKeeper sub-directory (key)"),
    Command(this, "zput", publishMessage, UnixLikeParams(Seq("key" -> true, "value" -> true), Seq("-t" -> "type")), "Sets a key-value pair in ZooKeeper"),
    Command(this, "zruok", ruok, UnixLikeParams(), help = "Checks the status of a Zookeeper instance (requires netcat)"),
    Command(this, "zstats", stats, UnixLikeParams(), help = "Returns the statistics of a Zookeeper instance (requires netcat)"),
    Command(this, "ztree", tree, UnixLikeParams(Seq("path" -> false)), help = "Retrieves Zookeeper directory structure"))

  /**
   * Returns a Zookeeper input source
   * @param url the given input URL (e.g. "zk:/messages/cache001")
   * @return the option of a Zookeeper input source
   */
  override def getInputSource(url: String): Option[InputSource] = None

  /**
   * Returns a Zookeeper output source
   * @param url the given output URL (e.g. "zk:/messages/cache001")
   * @return the option of a Zookeeper output source
   */
  override def getOutputSource(url: String): Option[ZookeeperOutputSource] = {
    url.extractProperty("zk:") map (new ZookeeperOutputSource(zk, _))
  }

  override def moduleName = "zookeeper"

  override def moduleLabel = "zk"

  override def prompt: String = zkProxy_? map (zk => s"$zkCwd") getOrElse zkCwd

  override def shutdown() = zkProxy_?.foreach(_.close())

  override def supportedPrefixes: Seq[String] = Seq("zk")

  def prevCwd: String = config.getOrElse("trifecta.zookeeper.prevCwd", "/")

  def prevCwd_=(path: String) = config.set("trifecta.zookeeper.prevCwd", path)

  def zkCwd: String = config.getOrElse("trifecta.zookeeper.cwd", "/")

  def zkCwd_=(path: String) = config.set("trifecta.zookeeper.cwd", path)

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
      case path => zkExpandKeyToPath(path)
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
   * @example zconnect localhost:2181
   */
  def connect(params: UnixLikeArgs): Unit = {
    // determine the requested end-point
    val connectionString = params.args match {
      case Nil => config.zooKeeperConnect
      case zconnectString :: Nil => zconnectString
      case _ => dieSyntax(params)
    }

    // connect to the remote peer
    zkProxy_?.foreach(_.close())
    zkProxy_? = Option(ZKProxy(connectionString))
  }

  /**
   * Deletes a key-value pair in ZooKeeper
   * @example zdel /consumer/GRP000a2ce
   */
  def delete(params: UnixLikeArgs): Boolean = {
    params.args match {
      case List(path) => zk.delete(zkExpandKeyToPath(path, zkCwd))
      case _ => dieSyntax(params)
    }
  }

  /**
   * Recursively deletes all children for a given key in ZooKeeper
   * @example zdelall /test
   */
  def deleteRecursively(params: UnixLikeArgs): Boolean = {
    params.args match {
      case List(path) => zk.deleteRecursive(zkExpandKeyToPath(path, zkCwd))
      case _ => dieSyntax(params)
    }
  }

  /**
   * Dumps the contents of a specific Zookeeper key to the console
   * @example zget /storm/workerbeats/my-test-topology-17-1407973634
   * @example zget /storm/workerbeats/my-test-topology-17-1407973634 -t json
   */
  def getData(params: UnixLikeArgs): Option[Any] = {
    // get the key
    params.args.headOption flatMap { key =>
      // convert the key to a fully-qualified path
      val path = zkExpandKeyToPath(key)

      // retrieve (or guess) the value's format
      val valueType = params("-f") getOrElse "bytes"

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
    val path = params.args.headOption map (zkExpandKeyToPath(_, zkCwd)) getOrElse zkCwd

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
    params.args.headOption map (key => zk.ensurePath(zkExpandKeyToPath(key))) getOrElse Nil
  }

  /**
   * "zexists" - Returns the status of a Zookeeper key if it exists
   * @example zexists /path/to/data
   */
  def pathExists(params: UnixLikeArgs): Boolean = {
    // get the argument
    params.args.headOption.exists { key =>
      // convert the key to a fully-qualified path
      val path = zkExpandKeyToPath(key)

      // perform the action
      zk.exists(path)
    }
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
        val path = zkExpandKeyToPath(key)

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
    val (host, port) = splitHostAndPort(params, zk.connectionString)
    ("echo ruok" #> s"nc $host $port").!!
  }

  /**
   * "zstats" - Returns the statistics of a Zookeeper instance
   * echo stat | nc zookeeper 2181
   */
  def stats(params: UnixLikeArgs): String = {
    import scala.sys.process._

    // echo stat | nc zookeeper 2181
    val (host, port) = splitHostAndPort(params, zk.connectionString)
    ("echo stat" #> s"nc $host $port").!!
  }

  /**
   * Splits the host and port arguments into a tuple
   * @param params the given [[UnixLikeArgs]]
   * @param endPoint the given end point (e.g. "localhost:2181")
   * @return the host and port arguments as a tuple (e.g. ("localhost", "2181"))
   */
  private def splitHostAndPort(params: UnixLikeArgs, endPoint: String): (String, String) = {
    endPoint.split("[:]").toList match {
      case host :: Nil => (host, "2181")
      case host :: port :: Nil => (host, port)
      case _ => dieSyntax(params)
    }
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
    val path = params.args.headOption map (zkExpandKeyToPath(_, zkCwd)) getOrElse zkCwd

    // perform the action
    val paths = unwind(path)
    paths.map(subPath => ZkItem(subPath, zk.getCreationTime(subPath).map(new Date(_))))
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

  /**
   * Returns the connected Zookeeper Proxy
   * @return the connected Zookeeper Proxy
   */
  private implicit def zk: ZKProxy = {
    zkProxy_? match {
      case Some(zk) => zk
      case None if KafkaSandbox.getInstance.isDefined =>
        zkProxy_? = KafkaSandbox.getInstance.map(kl => ZKProxy(kl.getConnectString))
        zkProxy_?.get
      case None =>
        val zk = ZKProxy(config.zooKeeperConnect)
        zkProxy_? = Option(zk)
        zk
    }
  }

}
