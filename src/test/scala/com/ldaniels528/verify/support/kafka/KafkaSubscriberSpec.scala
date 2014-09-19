package com.ldaniels528.verify.support.kafka

import com.ldaniels528.verify.support.kafka.KafkaMicroConsumer.BrokerDetails
import net.liftweb.json._
import org.scalatest.Matchers._
import org.scalatest.{FeatureSpec, GivenWhenThen}

/**
 * Kafka Subscriber Specification
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class KafkaSubscriberSpec() extends FeatureSpec with GivenWhenThen {
  implicit val formats = DefaultFormats

  info("As a KafkaSubscriber instance")
  info("I want to be able to consume Kafka messages")

  feature("The ability to decode Kafka broker details") {
    scenario("Decode a Kafka brokers to a case class") {
      Given("a JSON string containing the broker details")
      val jsonString = """{"jmx_port":9999,"timestamp":"1405818758964","host":"dev501","version":1,"port":9092}"""

      When("decoding the JSON string")
      val json = parse(jsonString)

      Then(s"the case class instance is created")
      val obj = json.extract[BrokerDetails]

      And(s"the case class instance contains the expected results")
      obj shouldBe BrokerDetails(jmx_port = 9999, timestamp = "1405818758964", host = "dev501", version = 1, port = 9092)
    }
  }

}
