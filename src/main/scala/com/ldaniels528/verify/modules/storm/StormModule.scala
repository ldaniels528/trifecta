package com.ldaniels528.verify.modules.storm

import backtype.storm.utils.{NimbusClient, Utils}
import com.ldaniels528.verify.VerifyShellRuntime
import com.ldaniels528.verify.modules.{Command, Module}
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._

/**
 * Apache Storm Module
 */
class StormModule(rt: VerifyShellRuntime) extends Module {
  private val logger = LoggerFactory.getLogger(getClass)
  private val stormConf = Utils.readStormConfig().asInstanceOf[java.util.Map[String, Any]]
  stormConf.put("nimbus.host", "dev501")
  private val client = NimbusClient.getConfiguredClient(stormConf).getClient

  // the name of the module
  val name = "storm"

  override def prompt: String = s"${rt.remoteHost}${rt.zkCwd}"

  // the bound commands
  val getCommands = Seq(
    Command(this, "sconf", showConfig, (Seq.empty, Seq.empty), help = "Lists the Storm configuration"),
    Command(this, "sdeploy", deployTopology, (Seq("jarfile", "topology"), Seq("arguments")), help = "Deploys a topology to the Storm server"),
    Command(this, "skill", killTopology, (Seq.empty, Seq("topologyName")), help = "Kills a running topology"),
    Command(this, "sls", listTopologies, (Seq.empty, Seq("prefix")), help = "Lists available topologies")
  )

  /**
   * Called when the application is shutting down
   */
  override def shutdown(): Unit = ()

  /**
   * "sdeploy" command - Deploys a topology to the Storm server
   * Example: sdeploy mytopology.jar myconfig.properties
   */
  def deployTopology(args: String*): String = {
    import scala.sys.process._

    // deploy the topology
    s"storm jar ${args mkString " "}".!!
  }

  /**
   * "skill" - Lists available topologies
   * @example {{ skill myTopology }}
   */
  def killTopology(args: String*): Unit = {
    val topologyName = args.head
    client.killTopology(topologyName)
  }

  /**
   * "sls" - Lists available topologies
   * @example {{ sls }}
   */
  def listTopologies(args: String*): Seq[TopologyDetails] = {
    client.getClusterInfo.get_topologies_iterator().toSeq map { t =>
      TopologyDetails(t.get_name, t.get_status, t.get_num_workers, t.get_uptime_secs)
    }
  }

  /**
   * "sconf" - Lists the Storm configuration
   * @example {{ sconf }}
   */
  def showConfig(args: String*): Seq[TopologyConfig] = {
    stormConf.toSeq map { case (k, v) => (k.toString, v)} sortBy (_._1) map { case (k, v) => TopologyConfig(k, v)}
  }

  case class TopologyConfig(key: String, value: Any)

  case class TopologyDetails(name: String, status: String, workers: Int, uptimeSecs: Long)

}
