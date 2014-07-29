package com.ldaniels528.verify.subsystems.storm

import com.ldaniels528.verify.subsystems.kafka._
import com.ldaniels528.verify.io.HttpResource
import com.ldaniels528.verify.io.Compression

/**
 * Web Service Reactive Push Bolt
 * @author lawrence.daniels@gmail.com
 */
class WebServerPushBolt(wsURL: String)
  extends backtype.storm.topology.IRichBolt with HttpResource with Compression {
  import java.util.{ Date, Map => JMap }
  import scala.util.{ Try, Success, Failure }
  import backtype.storm.spout.SpoutOutputCollector
  import backtype.storm.task.{ OutputCollector, TopologyContext }
  import backtype.storm.topology.OutputFieldsDeclarer
  import backtype.storm.tuple.{ Fields, Tuple, Values }
  import com.google.gson.{ Gson, GsonBuilder }
  import org.slf4j.LoggerFactory

  @transient private val logger = LoggerFactory.getLogger(classOf[KafkaSubscriber])
  private var conf: JMap[String, Object] = _
  private var collector: OutputCollector = _

  override def prepare(conf: JMap[_, _], context: TopologyContext, collector: OutputCollector) {
    this.conf = conf.asInstanceOf[JMap[String, Object]]
    this.collector = collector
  }

  override def execute(input: Tuple) {
    // get the JSON message
    Option(input.getBinaryByField("json")) match {
      case Some(message) =>
        // process the message
        val outcome = for {
          // uncompress the message
          jsonMessage <- Try(new String(deflate(message), "UTF-8"))

          // post the message
          response <- Try(httpPost(wsURL, jsonMessage))
        } yield response

        // handle the outcome
        outcome match {
          case Success(_) => collector.ack(input)
          case Failure(e) =>
            logger.error(s"[WebServerPushBolt] ERROR: ${e.getMessage} - JSON: ${deflate(message)}")
            collector.fail(input)
        }
      case None =>
        logger.warn(s"[WebServerPushBolt] No JSON data retrieved")
        collector.ack(input)
    }
  }

  override def cleanup() {
    // do nothing
  }

  override def declareOutputFields(declarer: OutputFieldsDeclarer) {
    // do nothing
  }

  override def getComponentConfiguration(): JMap[String, Object] = conf

}
