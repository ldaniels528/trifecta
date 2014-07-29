package com.ldaniels528.verify.subsystems.storm

import com.ldaniels528.verify.io.EndPoint
import com.ldaniels528.verify.subsystems.kafka.{ Broker, Topic }
import com.ldaniels528.verify.subsystems.zookeeper.ZKProxy
import com.ldaniels528.verify.util.VerifyUtils._

/**
 * Verify Storm Job
 * @author lawrence.daniels@gmail.com
 */
trait VerifyStormJob {
  import java.io.FileInputStream
  import java.util.Properties
  import backtype.storm.generated.StormTopology

  /**
   * Executes the topology described by the given topology builder
   */
  def runJob(name: String, topic: Topic, builder: (Topic, VerifyStormJobConfig) => StormTopology, args: Array[String]) {
    import backtype.storm.{ Config, LocalCluster, StormSubmitter }

    // get the parameters
    val configPath = args(0)
    val params = args.toSeq.tail
    val isLocal = params.exists(_ == "-local")
    val isDebug = params.exists(_ == "-debug")

    // load the configuration properties
    val p = new Properties()
    new FileInputStream(configPath) use (p.load)

    // get the topics & workers
    val workers = p.getProperty("servers.storm.workers", "4").toInt

    // create a new configuration
    val config = new Config()
    config.setNumWorkers(workers)
    config.setDebug(isDebug)

    // create the topology
    val topology = builder(topic, loadConfig(p))

    // launch the jobs
    if (isLocal) {
      // create the local cluster and begin processing
      val cluster = new LocalCluster()
      cluster.submitTopology(name, config, topology)
    } else {
      StormSubmitter.submitTopology(name, config, topology)
    }
  }

  /**
   * Retrieves the configuration corresponding to the given configuration file
   * @param configFile the given configuration file
   * @return the {@link VerifyStormJobConfig configuration}
   */
  def loadConfig(configFile: String): VerifyStormJobConfig = {
    val p = new Properties()
    new FileInputStream(configFile) use (p.load(_))
    loadConfig(p)
  }

  /**
   * Loads the server configuration
   * @param p the given {@link Properties properties}
   * @return the {@link VerifyStormJobConfig configuration}
   */
  def loadConfig(p: Properties): VerifyStormJobConfig = {
    // get the ZooKeeper end-point
    val zkEndPoint = EndPoint(p.getProperty("servers.zooKeeper"))

    // create the configuration instance
    VerifyStormJobConfig(
      cassandra = EndPoint(p.getProperty("servers.cassandra")),
      elasticSearch = EndPoint(p.getProperty("servers.elasticSearch")),
      zooKeeper = zkEndPoint,
      playReact = parseServerList(zkEndPoint, p.getProperty("location.servers.play.react")),
      playPublish = parseServerList(zkEndPoint, p.getProperty("location.servers.play.publish")),
      kafka = p.getProperty("servers.kafka.brokers").split("[,]").map(Broker(_)))
  }

  /**
   * Retrieve the list of play servers for REST callbacks
   */
  def parseServerList(zkEndPoint: EndPoint, key: String): Array[EndPoint] = {
    import org.apache.zookeeper.{ Watcher, WatchedEvent }

    // get the web service reactive push value
    val zkProxy = ZKProxy(zkEndPoint, new Watcher {
      override def process(event: WatchedEvent) {
        // do nothing
      }
    })

    // get the Play server end-points
    val serverList = for {
      // read the Play server list
      pairs <- zkProxy.readString(key)

      // parse the host:port pairs
    } yield pairs.split(",") map (EndPoint(_))

    serverList.getOrElse(throw new IllegalStateException(s"Could not deterine server end-points ($key)"))
  }

}