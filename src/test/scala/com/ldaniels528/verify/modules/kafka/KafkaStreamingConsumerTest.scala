package com.ldaniels528.verify.modules.kafka

import akka.actor.{Actor, ActorSystem, Props}
import com.ldaniels528.tabular.Tabular
import com.ldaniels528.verify.io.EndPoint
import com.ldaniels528.verify.modules.kafka.KafkaStreamingConsumer.{StreamedMessage, StreamingMessageConsumer}
import com.ldaniels528.verify.modules.kafka.KafkaStreamingConsumerTest._
import com.ldaniels528.verify.modules.zookeeper.ZKProxy
import com.ldaniels528.verify.util.VxUtils._
import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContext.Implicits._
import scala.concurrent.duration._

/**
 * Kafka Streaming Consumer Test
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class KafkaStreamingConsumerTest {

  //@Test
  def actorPatternTest(): Unit = {
    // setup the actor
    val system = ActorSystem("StreamingMessageSystem")
    val streamingActor = system.actorOf(Props[StreamingMessageActor], name = "streamingActor")

    // setup our Zookeeper connection
    val zkEndPoint = EndPoint("dev501", 2181)
    implicit val zk = ZKProxy(zkEndPoint)

    // reset the Kafka offsets
    resetOffsets("com.shocktrade.quotes.csv", "dev")

    // start streaming the data
    val consumer = KafkaStreamingConsumer(zkEndPoint, "dev")
    consumer.stream("com.shocktrade.quotes.csv", 4, streamingActor)

    Thread.sleep(15.seconds)
  }

  //@Test
  def observerPatternTest(): Unit = {
    // setup our Zookeeper connection
    val zkEndPoint = EndPoint("dev501", 2181)
    implicit val zk = ZKProxy(zkEndPoint)

    // reset the Kafka offsets
    resetOffsets("com.shocktrade.quotes.csv", "dev")

    // start streaming the data
    val consumer = KafkaStreamingConsumer(zkEndPoint, "dev")
    consumer.observe("com.shocktrade.quotes.csv", 4, new StreamingMessageConsumer {
      override def consume(message: StreamedMessage): Unit = {
        tabular.transform(Seq(message)) foreach logger.info
      }
    })

    Thread.sleep(15.seconds)
  }

  private def resetOffsets(topic: String, groupId: String)(implicit zk: ZKProxy): Unit = {
    val brokers = KafkaSubscriber.getBrokerList map (b => Broker(b.host, b.port))
    (0 to 3) foreach { partition =>
      new KafkaSubscriber(Topic(topic, partition), brokers, 1) use (_.commitOffsets(groupId, 0L, "Development offset"))
    }
  }


}

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
