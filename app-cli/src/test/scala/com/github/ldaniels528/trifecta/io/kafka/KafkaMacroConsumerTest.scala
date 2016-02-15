package com.github.ldaniels528.trifecta.io.kafka

import akka.actor.{Actor, ActorSystem, Props}
import com.github.ldaniels528.trifecta.io.kafka.KafkaMacroConsumerTest._
import com.github.ldaniels528.trifecta.io.zookeeper.ZKProxy
import com.github.ldaniels528.commons.helpers.ResourceHelper._
import com.github.ldaniels528.commons.helpers.TimeHelper.Implicits._
import com.github.ldaniels528.tabular.Tabular
import kafka.common.TopicAndPartition
import org.junit.{After, Before, Test}
import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContext.Implicits._
import scala.concurrent.duration._

/**
 * Kafka Streaming Consumer Test
 * @author lawrence.daniels@gmail.com
 */
class KafkaMacroConsumerTest {
  // setup our Zookeeper connection
  private val zkConnect = "dev501:2181"
  private implicit val zk = ZKProxy(zkConnect)
  private val consumerId = "dev"
  private val parallelism = 4

  @Before
  def setup(): Unit = resetOffsets("com.shocktrade.quotes.csv", partitions = 5, consumerId)

  @Test
  def actorPatternTest(): Unit = {
    // setup the actor
    val system = ActorSystem("StreamingMessageSystem")
    val streamingActor = system.actorOf(Props[StreamingMessageActor], name = "streamingActor")

    // start streaming the data
    val consumer = KafkaMacroConsumer(zkConnect, consumerId)
    consumer.stream("com.shocktrade.quotes.csv", parallelism, streamingActor)
  }

  @Test
  def iteratePatternTest(): Unit = {
    val consumer = KafkaMacroConsumer(zkConnect, consumerId)
    for (message <- consumer.iterate("com.shocktrade.quotes.csv", parallelism = 1)) {
      tabular.transform(Seq(message)) foreach logger.info
    }
  }

  @Test
  def observerPatternTest(): Unit = {
    // start streaming the data
    val consumer = KafkaMacroConsumer(zkConnect, consumerId)
    consumer.observe("com.shocktrade.quotes.csv", parallelism) { message =>
      tabular.transform(Seq(message)) foreach logger.info
    }
  }

  @After
  def waitForTestToComplete(): Unit = Thread.sleep(30.seconds)

  /**
   * Resets the offset for each partition of a topic
   */
  private def resetOffsets(topic: String, partitions: Int, groupId: String)(implicit zk: ZKProxy): Unit = {
    logger.info("Retrieving Kafka brokers...")
    val brokerDetails = KafkaMicroConsumer.getBrokerList
    tabular.transform(brokerDetails) foreach logger.info

    // extract just the broker objects
    val brokers = brokerDetails map (b => Broker(b.host, b.port))

    // reset each partitions
    (0 to (partitions - 1)) foreach { partition =>
      new KafkaMicroConsumer(TopicAndPartition(topic, partition), brokers) use (_.commitOffsets(groupId, 0L, "Development offset"))
    }
  }

}

/**
 * Kafka Streaming Consumer Test Companion Object
 * @author lawrence.daniels@gmail.com
 */
object KafkaMacroConsumerTest {
  private val logger = LoggerFactory.getLogger(getClass)
  private val tabular = new Tabular()

  class StreamingMessageActor() extends Actor {
    override def receive: Receive = {
      case message: StreamedMessage =>
        tabular.transform(Seq(message)) foreach logger.info
      case unknown =>
        unhandled(unknown)
    }
  }

}
