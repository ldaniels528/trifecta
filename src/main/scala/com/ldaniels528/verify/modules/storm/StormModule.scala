package com.ldaniels528.verify.modules.storm

import backtype.storm.generated.Nimbus
import backtype.storm.utils.{NimbusClient, Utils}
import com.ldaniels528.verify.VxRuntimeContext
import com.ldaniels528.verify.modules.{Command, Module}
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
  connect match {
    case Success(c) => client = Some(c)
    case Failure(e) =>
      val nimbusHost = Option(stormConf.get("nimbus.host")) map(_.asInstanceOf[String])
      logger.debug(s"Error connecting to Storm nimbus host ${if(nimbusHost.isDefined) s"(host: $nimbusHost)" else ""}", e)
  }

  // the bound commands
  override def getCommands = Seq(
    Command(this, "sconf", showConfig, (Seq.empty, Seq("key", "value")), help = "Lists, retrieves or sets the configuration keys"),
    Command(this, "sdeploy", deployTopology, (Seq("jarfile", "topology"), Seq("arguments")), help = "Deploys a topology to the Storm server (EXPERIMENTAL)"),
    Command(this, "sget", lookupTopology, (Seq("topologyName"), Seq.empty), help = "Retrieves the information for a topology"),
    Command(this, "skill", killTopology, (Seq.empty, Seq("topologyName")), help = "Kills a running topology"),
    Command(this, "sls", listTopologies, (Seq.empty, Seq("prefix")), help = "Lists available topologies")
  )

  override def getVariables: Seq[Variable] = Seq.empty

  override def moduleName = "storm"

  override def prompt: String = nimbusHost getOrElse super.prompt

  override def shutdown(): Unit = ()

  def nimbusHost: Option[String] = Option(stormConf.get("nimbus.host"))  map(_.asInstanceOf[String])

  /**
   * Retrieves the information for a topology
   * @example {{{ sget myTopology }}}
   */
  def lookupTopology(args: String*): Seq[TopologyInfo] = {
    val topologyName = args.head
    client.map(_.getTopology(topologyName)) map { t =>
      Seq(TopologyInfo(topologyName, t.get_bolts_size, t.get_spouts_size))
      /*
      val bolts = (t.get_bolts map { case (name, bolt) => TopologyInfo(name, "Bolt")}).toSeq
      val spouts = (t.get_spouts map { case (name, spout) => TopologyInfo(name, "Spout")}).toSeq
      bolts ++ spouts
      */
    } getOrElse Seq.empty
  }

  case class TopologyInfo(name: String, bolts: Int, spouts: Int)

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
    val topologyName = args.head
    client.foreach(_.killTopology(topologyName))
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

  case class TopologyDetails(name: String, id: String, status: String, workers: Int, executors: Int, tasks: Int, uptimeSecs: Long)

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
