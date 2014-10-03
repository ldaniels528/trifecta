package com.ldaniels528.trifecta.support.elasticsearch

import com.ldaniels528.tabular.Tabular
import org.scalatest.{FeatureSpec, GivenWhenThen}

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

/**
 * Elastic Search Specification
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class ElasticSearchDAOSpec() extends FeatureSpec with GivenWhenThen {
  val tabular = new Tabular()
  val client = new ElasticSearchDAO(new TxElasticSearchClient("dev501", 9200))

  info("As a user of ElasticSearch")
  info("I want to be able to execute ElasticSearch queries")

  /*
  feature("Ability to retrieve health information") {
    scenario("Get the cluster's health") {
      When("a call is made for the cluster's health")
      val status = wait(client.health())

      Then("the results should match the expectation")
      tabular.transform(Seq(status)) foreach (s => info(s))
    }
  }

  feature("Ability to create an index") {
    scenario("Create an index called 'foo'") {
      When("an index creation is attempted")
      val response = wait(client.createIndex(name = "foo1"))

      Then("the results should match the expectation")
      info(s"response: $response")
    }
  }

  feature("Ability to search for all documents") {
    scenario("Search for all documents within an index") {
      When("searching for all documents within index 'foo2'")
      val response = wait(client.matchAll("foo1"))

      Then("the status code should be 200 OK")
      info(s"ResponseBody: ${response.getResponseBody}")
      response.getStatusCode should be >= 200
      response.getStatusCode should be < 300
    }
  }*/

  def wait[A](task: Future[A]): A = Await.result(task, 15.seconds)

}
