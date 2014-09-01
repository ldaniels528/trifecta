package com.ldaniels528.verify.modules.kafka

import akka.actor.{Actor, ActorSystem, Props}
import com.ldaniels528.tabular.Tabular
import com.ldaniels528.verify.io.EndPoint
import com.ldaniels528.verify.modules.kafka.KafkaStreamingConsumer.{StreamedMessage, StreamingMessageObserver}
import com.ldaniels528.verify.modules.kafka.KafkaStreamingConsumerTest._
import com.ldaniels528.verify.modules.zookeeper.ZKProxy
import com.ldaniels528.verify.util.VxUtils._
import org.junit.{After, Before, Test}
import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContext.Implicits._
import scala.concurrent.duration._

/**
 * Kafka Streaming Consumer Test
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class KafkaStreamingConsumerTest {
  // setup our Zookeeper connection
  private val zkEndPoint = EndPoint("dev501", 2181)
  private implicit val zk = ZKProxy(zkEndPoint)

  @Before
  def setup(): Unit = resetOffsets("com.shocktrade.quotes.csv", 4, "dev")

  @Test
  def actorPatternTest(): Unit = {
    // setup the actor
    val system = ActorSystem("StreamingMessageSystem")
    val streamingActor = system.actorOf(Props[StreamingMessageActor], name = "streamingActor")

    // start streaming the data
    val consumer = KafkaStreamingConsumer(zkEndPoint, "dev")
    consumer.stream("com.shocktrade.quotes.csv", 4, streamingActor)
  }

  @Test
  def observerPatternTest(): Unit = {
    // start streaming the data
    val consumer = KafkaStreamingConsumer(zkEndPoint, "dev")
    consumer.observe("com.shocktrade.quotes.csv", 4, new StreamingMessageObserver {
      override def consume(message: StreamedMessage) = {
        tabular.transform(Seq(message)) foreach logger.info
        false
      }
    })
  }

  @After
  def waitForTestToComplete(): Unit = Thread.sleep(30.seconds)

  /**
   * Resets the offset for each partition of a topic
   */
  private def resetOffsets(topic: String, partitions: Int, groupId: String)(implicit zk: ZKProxy): Unit = {
    logger.info("Retrieving Kafka brokers...")
    val brokerDetails = KafkaSubscriber.getBrokerList
    tabular.transform(brokerDetails) foreach logger.info

    // extract just the broker objects
    val brokers = brokerDetails map (b => Broker(b.host, b.port))

    // reset each partitions
    (0 to (partitions - 1)) foreach { partition =>
      new KafkaSubscriber(Topic(topic, partition), brokers, 1) use (_.commitOffsets(groupId, 0L, "Development offset"))
    }
  }

}

/**
 * Kafka Streaming Consumer Test Companion Object
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
object KafkaStreamingConsumerTest {
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
