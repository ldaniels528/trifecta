package com.ldaniels528.trifecta.modules.storm

import java.net.{URL, URLClassLoader}

import backtype.storm.generated.{Grouping, Nimbus}
import backtype.storm.utils.{NimbusClient, Utils}
import com.ldaniels528.trifecta.modules.io.OutputWriter
import com.ldaniels528.trifecta.{TxConfig, TxRuntimeContext}
import com.ldaniels528.trifecta.command.{Command, SimpleParams, UnixLikeArgs}
import com.ldaniels528.trifecta.modules.Module
import com.ldaniels528.trifecta.vscript.Variable
import net.liftweb.json.JsonAST.JValue
import net.liftweb.json._
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._
import scala.util.{Failure, Success, Try}

/**
 * Apache Storm Module
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class StormModule(config: TxConfig) extends Module {
  private val logger = LoggerFactory.getLogger(getClass)
  private val stormConf = Utils.readStormConfig().asInstanceOf[java.util.Map[String, Any]]
  private var client: Option[Nimbus.Client] = None

  // load Storm-specific properties
  config.configProps foreach { case (k, v) =>
    if (k.startsWith("storm|")) {
      stormConf.put(k.drop(6), v)
    }
  }

  // connect to the server
  createConnection(UnixLikeArgs(commandName = None, args = Nil))

  // the bound commands
  override def getCommands(implicit rt: TxRuntimeContext): Seq[Command] = Seq(
    Command(this, "sbolts", getTopologyBolts, SimpleParams(Seq("topologyID"), Seq.empty), help = "Retrieves the list of bolts for s given topology by ID", promptAware = true),
    Command(this, "sconf", showConfig, SimpleParams(Seq.empty, Seq("key", "value")), help = "Lists, retrieves or sets the configuration keys", promptAware = true),
    Command(this, "sconnect", createConnection, SimpleParams(Seq.empty, Seq("nimbusHost")), help = "Establishes (or re-establishes) a connect to the Storm Nimbus Host", promptAware = true),
    Command(this, "sdeploy", deployTopology, SimpleParams(Seq("jarfile", "topology"), Seq("arguments")), help = "Deploys a topology to the Storm server (EXPERIMENTAL)", promptAware = true, undocumented = true),
    Command(this, "sget", getTopologyInfo, SimpleParams(Seq("topologyID"), Seq.empty), help = "Retrieves the information for a topology", promptAware = true),
    Command(this, "skill", killTopology, SimpleParams(Seq("topologyID"), Seq.empty), help = "Kills a running topology", promptAware = true),
    Command(this, "sls", listTopologies, SimpleParams(Seq.empty, Seq("prefix")), help = "Lists available topologies", promptAware = true),
    Command(this, "spouts", getTopologySpouts, SimpleParams(Seq("topologyID"), Seq.empty), help = "Retrieves the list of spouts for a given topology by ID", promptAware = true),
    Command(this, "srun", runTopology, SimpleParams(Seq("jarPath", "className"), Seq("arg0", "arg1", "arg2")), help = "Retrieves the list of spouts for a given topology by ID", promptAware = true, undocumented = true)
  )

  /**
   * Returns an output writer for the given module
   * @param path the given output path
   * @return the option of an output writer
   */
  override def getOutput(path: String): Option[OutputWriter] = None

  override def getVariables: Seq[Variable] = Nil

  override def moduleName = "storm"

  override def prompt: String = nimbusHost getOrElse super.prompt

  override def shutdown(): Unit = ()

  def nimbusHost: Option[String] = Option(stormConf.get("nimbus.host")) map (_.asInstanceOf[String])

  def nimbusHost_=(host: String) = stormConf.put("nimbus.host", host)

  /**
   * Establishes (or re-establishes) a connect to the Storm Nimbus Host
   * @example sconnect
   */
  def createConnection(params: UnixLikeArgs) = {
    val myNimbusHost = params.args.headOption

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
   * @example sget nm-traffic-rate-aggregation-17-1407973634
   */
  def getTopologyInfo(params: UnixLikeArgs): Seq[TopologyInfo] = {
    // get the topology ID
    val topologyId = params.args.head

    client.map(_.getTopology(topologyId)) map { topology =>
      Seq(TopologyInfo(topologyId, topology.get_bolts_size, topology.get_spouts_size))
    } getOrElse Seq.empty
  }

  case class TopologyInfo(topologyId: String, bolts: Int, spouts: Int)

  /**
   * "sbolts" - Retrieves the list of bolts for a given topology by ID
   * @example sbolts nm-traffic-rate-aggregation-17-1407973634
   */
  def getTopologyBolts(params: UnixLikeArgs): Seq[BoltInfo] = {
    import net.liftweb.json._

    // get the topology ID
    val topologyId = params.args.head

    client.map(_.getTopology(topologyId)) map { topology =>
      topology.get_bolts.toSeq filterNot { case (name, _) => name.startsWith("__")} sortBy { case (name, _) => name} map {
        case (name, bolt) =>
          //println(s"bolt.get_common.get_json_conf = ${bolt.get_common().get_json_conf()}")
          val jsConf = parse(bolt.get_common.get_json_conf)
          val tickTupleFreq = getStringValue(jsConf \ "topology.tick.tuple.freq.secs")

          val result = (bolt.get_common.get_inputs flatMap { case (id, grouping) =>
            //println(s"\tbolt.get_common.get_inputs: ${id.get_componentId} = $grouping [${grouping.getClass.getName}]")
            for {
              groupId <- Option(id.get_componentId)
              (groupingFields, groupingType) <- getGroupingValue(grouping)
            } yield (groupId, groupingFields, groupingType)
          }).headOption

          // get the group ID and grouping fields
          val (groupId, groupingFields, groupingType) = result match {
            case Some((aGroupId, aGrouping, aGroupType)) => (Option(aGroupId), Option(aGrouping), Option(aGroupType))
            case None => (None, None, None)
          }

          BoltInfo(name, Option(bolt.get_common) map (_.get_parallelism_hint), groupId, groupingFields, groupingType, tickTupleFreq)
      }
    } getOrElse Nil
  }

  case class BoltInfo(name: String, parallelism: Option[Int], input: Option[String], groupingFields: Option[String], groupingType: Option[String], tickTupleFreq: Option[String])

  private def getStringValue(jsConf: JValue): Option[String] = {
    implicit val formats = DefaultFormats

    Try(jsConf.extract[String]) match {
      case Success(v) => Some(v)
      case Failure(e) => None
    }
  }

  private def getGroupingValue(grouping: Grouping): Option[(String, String)] = {
    if (grouping.is_set_fields()) Option(grouping.get_fields) map (g => (g.mkString(", "), "Fields"))
    else if (grouping.is_set_all()) Option(grouping.get_all) map (g => (g.toString, "All"))
    else if (grouping.is_set_local_or_shuffle()) Option(grouping.get_local_or_shuffle()) map (g => ("", "Local/Shuffle"))
    else None
  }

  /**
   * "spouts" - Retrieves the list of spouts for a given topology by ID
   * @example sbolts nm-traffic-rate-aggregation-17-1407973634
   */
  def getTopologySpouts(params: UnixLikeArgs): Seq[SpoutInfo] = {
    // get the topology ID
    val topologyId = params.args.head

    client.map(_.getTopology(topologyId)) map { topology =>
      topology.get_spouts.toSeq sortBy { case (name, _) => name} map { case (name, spout) =>
        val jsConf = parse(spout.get_common.get_json_conf)
        val tickTupleFreq = getStringValue(jsConf \ "topology.tick.tuple.freq.secs")
        SpoutInfo(name, Option(spout.get_common) map (_.get_parallelism_hint), tickTupleFreq)
      }
    } getOrElse Seq.empty
  }

  case class SpoutInfo(name: String, parallelism: Option[Int], tickTupleFreq: Option[String])

  /**
   * "sdeploy" command - Deploys a topology to the Storm server
   * @example sdeploy mytopology.jar myconfig.properties
   */
  def deployTopology(params: UnixLikeArgs): String = {
    import scala.sys.process._

    // deploy the topology
    s"storm jar ${params.args mkString " "}".!!
  }

  /**
   * "skill" - Lists available topologies
   * @example skill myTopology
   */
  def killTopology(params: UnixLikeArgs): Unit = {
    // get the topology ID
    val topologyID = params.args.head

    // kill the topology
    client.foreach(_.killTopology(topologyID))
  }

  /**
   * "sls" - Lists available topologies
   * @example {{ sls }}
   */
  def listTopologies(args: UnixLikeArgs): Seq[TopologyDetails] = {
    client.map(_.getClusterInfo.get_topologies_iterator().toSeq map { t =>
      TopologyDetails(t.get_name, t.get_id, t.get_status, t.get_num_workers, t.get_num_executors, t.get_num_tasks, t.get_uptime_secs)
    }) getOrElse Seq.empty
  }

  case class TopologyDetails(name: String, topologyId: String, status: String, workers: Int, executors: Int, tasks: Int, uptimeSecs: Long)

  /**
   * "srun" - Runs as topology in a local cluster
   * @example srun shocktrade-etl.jar com.shocktrade.etl.QuotesTopology -local
   */
  def runTopology(args: UnixLikeArgs): Unit = {
    // get the parameters
    val Seq(jarPath, className, _*) = args.args.toSeq
    val params = args.args.drop(2)

    val classUrls = Array(new URL(s"file://$jarPath"))
    val classLoader = new URLClassLoader(classUrls)
    val classWithMain = classLoader.loadClass(className)
    val mainMethod = classWithMain.getMethod("main", classOf[Array[String]])
    mainMethod.invoke(null, params: _*)
    ()
  }

  /**
   * "sconf" - Lists the Storm configuration
   * @example {{ sconf }}
   */
  def showConfig(params: UnixLikeArgs): Seq[TopologyConfig] = {
    params.args.toList match {
      case Nil =>
        stormConf.toSeq map { case (k, v) => (k.toString, v)} sortBy (_._1) map { case (k, v) => TopologyConfig(k, v)}
      case key :: Nil =>
        val value = stormConf.get(key)
        Seq(TopologyConfig(key, value))
      case key :: value :: Nil =>
        stormConf.put(key, value)
        Seq(TopologyConfig(key, value))
      case _ =>
        dieSyntax(params)
    }
  }

  private def connect: Try[Nimbus.Client] = Try(NimbusClient.getConfiguredClient(stormConf).getClient)

  case class TopologyConfig(key: String, value: Any)

}
