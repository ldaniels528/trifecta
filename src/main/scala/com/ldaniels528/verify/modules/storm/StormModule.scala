package com.ldaniels528.verify.modules.storm

import backtype.storm.generated.Nimbus
import backtype.storm.utils.{NimbusClient, Utils}
import com.ldaniels528.verify.VerifyShellRuntime
import com.ldaniels528.verify.modules.{Command, Module}
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._
import scala.util.{Failure, Success, Try}

/**
 * Apache Storm Module
 */
class StormModule(rt: VerifyShellRuntime) extends Module {
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
  }

  val name = "storm"

  override def prompt: String = s"${rt.remoteHost}${rt.zkCwd}"

  // the bound commands
  val getCommands = Seq(
    Command(this, "sconf", showConfig, (Seq.empty, Seq("key", "value")), help = "Lists, retrieves or sets the configuration keys"),
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
    client.foreach(_.killTopology(topologyName))
  }

  /**
   * "sls" - Lists available topologies
   * @example {{ sls }}
   */
  def listTopologies(args: String*): Seq[TopologyDetails] = {
    client.map(_.getClusterInfo.get_topologies_iterator().toSeq map { t =>
      TopologyDetails(t.get_name, t.get_status, t.get_num_workers, t.get_uptime_secs)
    }) getOrElse Seq.empty
  }

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

  case class TopologyDetails(name: String, status: String, workers: Int, uptimeSecs: Long)

}
