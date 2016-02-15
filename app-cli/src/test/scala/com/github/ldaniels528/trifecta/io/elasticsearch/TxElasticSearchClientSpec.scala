package com.github.ldaniels528.trifecta.io.elasticsearch

import com.github.ldaniels528.tabular.Tabular
import org.scalatest.{FeatureSpec, GivenWhenThen}

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

/**
 * Elastic Search Client Specification
 * @author lawrence.daniels@gmail.com
 */
class TxElasticSearchClientSpec() extends FeatureSpec with GivenWhenThen {
  val tabular = new Tabular()
  val client = new TxElasticSearchClient("dev501", 9200)

  info("As a user of ElasticSearch")
  info("I want to be able to execute ElasticSearch queries")

  /*
  feature("Ability to retrieve health information") {
    scenario("Get the cluster's health") {
      When("a call is made for the cluster's health")
      val result = wait(client.health)

      Then("the status code should be 200 OK")
      info(s"ResponseBody: $result")
    }
  }

  feature("Ability to retrieve nodes information") {
    scenario("Get the cluster's nodes") {
      When("a call is made for the cluster's nodes")
      val result = wait(client.nodes)

      Then("the status code should be 200 OK")
      info(s"ResponseBody: $result")
    }
  }

  feature("Ability to count documents") {
    scenario("Count all documents within an index") {
      When("attempting to count all documents within an index")
      val result = wait(client.count("quotes", "quote"))

      Then("the status code should be 200 OK")
      info(s"ResponseBody:  $result")
    }

    scenario("Count matching documents within an index") {
      When("attempting to count matching documents within an index")
      val result = wait(client.count("quotes", "quote", """{ "query" : { "term" : { "symbol" : "AAPL" } } }"""))

      Then("the status code should be 200 OK")
      info(s"ResponseBody:  $result")
    }
  }

  feature("Ability to search for documents") {
    scenario("Fetch all documents within an index") {
      When("attempting to fetch all documents within an index")
      val result = wait(client.matchAll("quotes"))

      Then("the status code should be 200 OK")
      info(s"ResponseBody:  $result")
    }

    scenario("Fetch a document by it's id") {
      When("attempting to fetch document by it's id")
      val result = wait(client.search("quotes", "quote", "symbol" -> "AAPL"))

      Then("the status code should be 200 OK")
      info(s"ResponseBody:  $result")
    }
  }  */

  def wait[A](task: Future[A]): A = Await.result(task, 15.seconds)

}
