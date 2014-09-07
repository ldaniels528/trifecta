package com.ldaniels528.verify.modules.storm

import backtype.storm.generated.Nimbus
import backtype.storm.utils.{NimbusClient, Utils}
import com.ldaniels528.verify.VxRuntimeContext
import com.ldaniels528.verify.modules.{SimpleParams, Command, Module}
import com.ldaniels528.verify.vscript.Variable
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._
import scala.util.{Failure, Success, Try}

/**
 * Apache Storm Module
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class StormModule(rt: VxRuntimeContext) extends Module {
  private val logger = LoggerFactory.getLogger(getClass)
  private val stormConf = Utils.readStormConfig().asInstanceOf[java.util.Map[String, Any]]
  private var client: Option[Nimbus.Client] = None

  // load Storm-specific properties
  rt.configProps foreach { case (k, v) =>
    if (k.startsWith("storm|")) {
      stormConf.put(k.drop(6), v)
    }
  }

  // connect to the server
  createConnection()

  // the bound commands
  override def getCommands: Seq[Command] = Seq(
    Command(this, "sbolts", getTopologyBolts, SimpleParams(Seq("topologyID"), Seq.empty), help = "Retrieves the list of bolts for s given topology by ID", promptAware = true),
    Command(this, "sconf", showConfig, SimpleParams(Seq.empty, Seq("key", "value")), help = "Lists, retrieves or sets the configuration keys", promptAware = true),
    Command(this, "sconnect", createConnection, SimpleParams(Seq.empty, Seq("nimbusHost")), help = "Establishes (or re-establishes) a connect to the Storm Nimbus Host", promptAware = true),
    Command(this, "sdeploy", deployTopology, SimpleParams(Seq("jarfile", "topology"), Seq("arguments")), help = "Deploys a topology to the Storm server (EXPERIMENTAL)", promptAware = true, undocumented = true),
    Command(this, "sget", getTopologyInfo, SimpleParams(Seq("topologyID"), Seq.empty), help = "Retrieves the information for a topology", promptAware = true),
    Command(this, "skill", killTopology, SimpleParams(Seq("topologyID"), Seq.empty), help = "Kills a running topology", promptAware = true),
    Command(this, "sls", listTopologies, SimpleParams(Seq.empty, Seq("prefix")), help = "Lists available topologies", promptAware = true),
    Command(this, "spouts", getTopologySpouts, SimpleParams(Seq("topologyID"), Seq.empty), help = "Retrieves the list of spouts for a given topology by ID", promptAware = true)
  )

  override def getVariables: Seq[Variable] = Seq.empty

  override def moduleName = "storm"

  override def prompt: String = nimbusHost getOrElse super.prompt

  override def shutdown(): Unit = ()

  def nimbusHost: Option[String] = Option(stormConf.get("nimbus.host")) map (_.asInstanceOf[String])

  def nimbusHost_=(host: String) = stormConf.put("nimbus.host", host)

  /**
   * Establishes (or re-establishes) a connect to the Storm Nimbus Host
   * @example {{{ sconnect }}}
   */
  def createConnection(args: String*) = {
    val myNimbusHost = args.headOption

    // optionally set the Nimus host
    myNimbusHost.foreach(nimbusHost = _)

    // establish the connection
    connect match {
      case Success(c) => client = Some(c)
      case Failure(e) =>
        logger.debug(s"Error connecting to Storm nimbus host ${if (nimbusHost.isDefined) s"(host: $nimbusHost)" else ""}", e)
    }
  }

  /**
   * "sget" - Retrieves the information for a topology by ID
   * @example {{{ sget nm-traffic-rate-aggregation-17-1407973634 }}}
   */
  def getTopologyInfo(args: String*): Seq[TopologyInfo] = {
    // get the topology ID
    val topologyId = args.head

    client.map(_.getTopology(topologyId)) map { t =>
      Seq(TopologyInfo(topologyId, t.get_bolts_size, t.get_spouts_size))
    } getOrElse Seq.empty
  }

  case class TopologyInfo(topologyId: String, bolts: Int, spouts: Int)

  /**
   * "sbolts" - Retrieves the list of bolts for a given topology by ID
   * @example {{{ sbolts nm-traffic-rate-aggregation-17-1407973634 }}}
   */
  def getTopologyBolts(args: String*): Seq[BoltSpoutInfo] = {
    // get the topology ID
    val topologyId = args.head

    client.map(_.getTopology(topologyId)) map { t =>
      t.get_bolts.toSeq map { case (name, bolt) => BoltSpoutInfo(topologyId, name)}
    } getOrElse Seq.empty
  }

  /**
   * "spouts" - Retrieves the list of spouts for a given topology by ID
   * @example {{{ sbolts nm-traffic-rate-aggregation-17-1407973634 }}}
   */
  def getTopologySpouts(args: String*): Seq[BoltSpoutInfo] = {
    // get the topology ID
    val topologyId = args.head

    client.map(_.getTopology(topologyId)) map { t =>
      t.get_spouts.toSeq map { case (name, spout) => BoltSpoutInfo(topologyId, name)}
    } getOrElse Seq.empty
  }

  case class BoltSpoutInfo(topologyId: String, name: String)

  /**
   * "sdeploy" command - Deploys a topology to the Storm server
   * @example {{{ sdeploy mytopology.jar myconfig.properties }}}
   */
  def deployTopology(args: String*): String = {
    import scala.sys.process._

    // deploy the topology
    s"storm jar ${args mkString " "}".!!
  }

  /**
   * "skill" - Lists available topologies
   * @example {{{ skill myTopology }}}
   */
  def killTopology(args: String*): Unit = {
    // get the topology ID
    val topologyID = args.head

    // kill the topology
    client.foreach(_.killTopology(topologyID))
  }

  /**
   * "sls" - Lists available topologies
   * @example {{ sls }}
   */
  def listTopologies(args: String*): Seq[TopologyDetails] = {
    client.map(_.getClusterInfo.get_topologies_iterator().toSeq map { t =>
      TopologyDetails(t.get_name, t.get_id, t.get_status, t.get_num_workers, t.get_num_executors, t.get_num_tasks, t.get_uptime_secs)
    }) getOrElse Seq.empty
  }

  case class TopologyDetails(name: String, topologyId: String, status: String, workers: Int, executors: Int, tasks: Int, uptimeSecs: Long)

  /**
   * "sconf" - Lists the Storm configuration
   * @example {{ sconf }}
   */
  def showConfig(args: String*): Seq[TopologyConfig] = {
    args.toList match {
      case Nil =>
        stormConf.toSeq map { case (k, v) => (k.toString, v)} sortBy (_._1) map { case (k, v) => TopologyConfig(k, v)}
      case key :: Nil =>
        val value = stormConf.get(key)
        Seq(TopologyConfig(key, value))
      case key :: value :: Nil =>
        stormConf.put(key, value)
        Seq(TopologyConfig(key, value))
      case _ =>
        throw new IllegalArgumentException("Usage: sconf [<key> [<value>]]")
    }
  }

  private def connect: Try[Nimbus.Client] = Try(NimbusClient.getConfiguredClient(stormConf).getClient)

  case class TopologyConfig(key: String, value: Any)

}
