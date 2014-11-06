package com.ldaniels528.trifecta.support.storm

import java.util
import java.util.{Map => JMap}

import backtype.storm.spout.SpoutOutputCollector
import backtype.storm.task.TopologyContext
import backtype.storm.topology.OutputFieldsDeclarer
import backtype.storm.topology.base.BaseRichSpout
import backtype.storm.tuple.{Fields, Values}
import com.ldaniels528.trifecta.support.kafka.{StreamedMessage, KafkaMacroConsumer}
import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContext.Implicits._
import scala.concurrent.Future
import scala.language.postfixOps

/**
 * Verify Kafka Spout
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class VxKafkaSpout(zookeeperConnect: String, topic: String, parallelism: Int, consumerId: String, outputField: String)
  extends BaseRichSpout {
  @transient private lazy val logger = LoggerFactory.getLogger(getClass)
  @transient private lazy val consumer = KafkaMacroConsumer(zookeeperConnect, consumerId)
  @transient private lazy val queue = new util.LinkedList[StreamedMessage]()
  private var conf: JMap[String, Object] = _
  private var collector: SpoutOutputCollector = _
  private var once = true

  override def activate() = ()

  override def close() = consumer.close()

  override def deactivate() = ()

  override def declareOutputFields(declarer: OutputFieldsDeclarer) = declarer.declare(new Fields(outputField))

  override def getComponentConfiguration: JMap[String, Object] = conf

  override def open(conf: JMap[_, _], context: TopologyContext, collector: SpoutOutputCollector) = {
    this.collector = collector
    this.conf = conf.asInstanceOf[JMap[String, Object]]

    // schedule the events
    if (once) {
      once = false
      startConsuming()
      ()
    }
  }

  override def nextTuple() {
    Option(queue.poll()) foreach { msg =>
      collector.emit(new Values(msg.message), msg.key)
    }
    ()
  }

  private def startConsuming(): Future[Unit] = {
    Future {
      consumer.observe(topic, parallelism) { message =>
        queue.add(message)
        ()
      }
    }
  }

}
