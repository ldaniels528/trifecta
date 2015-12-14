package com.github.ldaniels528.trifecta.io.kafka

import com.github.ldaniels528.trifecta.io.kafka.KafkaMicroConsumer.BrokerDetails
import net.liftweb.json._
import org.scalatest.Matchers._
import org.scalatest.{FeatureSpec, GivenWhenThen}

/**
 * Kafka Subscriber Specification
 * @author lawrence.daniels@gmail.com
 */
class KafkaSubscriberSpec() extends FeatureSpec with GivenWhenThen {
  implicit val formats = DefaultFormats

  info("As a KafkaSubscriber instance")
  info("I want to be able to consume Kafka messages")

  feature("The ability to decode Kafka broker details") {
    scenario("Decode a Kafka brokers to a case class") {
      Given("a JSON string containing the broker details")
      val jsonString = """{"jmx_port":9999,"timestamp":"1405818758964","host":"dev501","version":1,"port":9092}"""

      When("parsing the JSON string")
      val json = parse(jsonString)

      Then(s"the case class instance is created")
      val obj = json.extract[BrokerDetails]

      And(s"the case class instance contains the expected results")
      obj shouldBe BrokerDetails(jmx_port = 9999, timestamp = "1405818758964", host = "dev501", version = 1, port = 9092)
    }
  }

  feature("The ability to decode Kafka Partition Manager consumer details") {
    scenario("Decode a Kafka Partition Manager consumer details to a JValue") {
      Given("a JSON string containing the consumer details")
      val jsonString = """
                         |    {
                         |        "topology":{
                         |          "id":"a46e3b1e-8a67-4aa0-9d8e-44b4e9bad9b9",
                         |          "name":"NetworkMonitoringTrafficRateAggregation"
                         |        },
                         |        "offset":43710000,
                         |        "partition":1,
                         |        "broker":{
                         |          "host":"vsccrtc204-brn1.rtc.vrsn.com",
                         |          "port":9092
                         |        },
                         |        "topic":"Verisign.test.iota.rtc.listener.all.hydra"
                         |    }""".stripMargin

      When("parsing the JSON string")
      val json = parse(jsonString)
      val id = (json \ "topology" \ "id").extract[String]
      val name = (json \ "topology" \ "name").extract[String]
      val topic = (json \ "topic").extract[String]
      val offset = (json \ "offset").extract[Long]
      val partition = (json \ "partition").extract[Int]
      val brokerHost = (json \ "broker" \ "host").extract[String]
      val brokerPort = (json \ "broker" \ "port").extract[Int]


      info(s"id = $id")

      Then(s"the case class instance is created")
      //val obj = json.extract[BrokerDetails]

      And(s"the case class instance contains the expected results")
      //obj shouldBe BrokerDetails(jmx_port = 9999, timestamp = "1405818758964", host = "dev501", version = 1, port = 9092)
    }
  }

}
