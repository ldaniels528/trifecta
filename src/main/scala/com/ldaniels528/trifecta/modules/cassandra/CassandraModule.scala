package com.ldaniels528.trifecta.modules.cassandra

import com.ldaniels528.trifecta.command.{Command, UnixLikeArgs, UnixLikeParams}
import com.ldaniels528.trifecta.modules.Module
import com.ldaniels528.trifecta.support.cassandra.Casserole
import com.ldaniels528.trifecta.support.io.{InputSource, OutputSource}
import com.ldaniels528.trifecta.util.EndPoint
import com.ldaniels528.trifecta.vscript.Variable
import com.ldaniels528.trifecta.{TxConfig, TxRuntimeContext}

/**
 * Apache Cassandra Module
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class CassandraModule(config: TxConfig) extends Module {
  private var conn_? : Option[Casserole] = None

  /**
   * Returns the commands that are bound to the module
   * @return the commands that are bound to the module
   */
  override def getCommands(implicit rt: TxRuntimeContext): Seq[Command] = Seq(
    Command(this, "cqconnect", connect, UnixLikeParams(Seq("host" -> false, "port" -> false)), help = "Establishes a connection to Cassandra"))

  /**
   * Attempts to retrieve an input source for the given URL
   * @param url the given input URL
   * @return the option of an input source
   */
  override def getInputSource(url: String): Option[InputSource] = None

  /**
   * Attempts to retrieve an output source for the given URL
   * @param url the given output URL
   * @return the option of an output source
   */
  override def getOutputSource(url: String): Option[OutputSource] = None

  /**
   * Returns the variables that are bound to the module
   * @return the variables that are bound to the module
   */
  override def getVariables: Seq[Variable] = Nil

  /**
   * Returns the name of the prefix (e.g. Seq("file"))
   * @return the name of the prefix
   */
  override def supportedPrefixes: Seq[String] = Nil

  /**
   * Returns the label of the module (e.g. "kafka")
   * @return the label of the module
   */
  override def moduleLabel: String = "cql"

  /**
   * Returns the name of the module (e.g. "kafka")
   * @return the name of the module
   */
  override def moduleName: String = "cassandra"

  /**
   * Called when the application is shutting down
   */
  override def shutdown(): Unit = ()

  /**
   * Establishes a connection to Zookeeper
   * @example cqconnect
   * @example cqconnect localhost
   */
  private def connect(params: UnixLikeArgs): Unit = {
    // determine the requested end-point
    val endPoint = params.args match {
      case Nil => EndPoint(config.zooKeeperConnect)
      case path :: Nil => EndPoint(path)
      case path :: port :: Nil => EndPoint(path, parseInt("port", port))
      case _ => dieSyntax(params)
    }

    // connect to the remote peer
    conn_?.foreach(_.close())
    conn_? = Option(Casserole(Seq(endPoint.host)))
  }

}
