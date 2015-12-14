package com.github.ldaniels528.trifecta.io.elasticsearch

import org.scalatest.{FeatureSpec, GivenWhenThen}

/**
 * Elastic Search Specification
 * @author lawrence.daniels@gmail.com
 */
class ElasticSearchSpec() extends FeatureSpec with GivenWhenThen {
  /*
  val tabular = new Tabular()
  val client = new TxElasticSearchClient("dev501", 9200)
  val esDAO = new ElasticSearchDAO(client)

  info("As a user of ElasticSearch")
  info("I want to be able to execute ElasticSearch queries")

  feature("Ability to retrieve health information") {
    scenario("Get the cluster's health") {
      When("a call is made for the cluster's health")
      val response = wait(client.health())

      Then("the status code should be 200 OK")
      info(s"ResponseBody: ${response.getResponseBody}")
      response.getStatusCode should be >= 200
      response.getStatusCode should be < 300
    }
  }

  feature("Ability to create an index") {
    scenario("Create an index called 'foo2'") {
      When("an index creation is attempted")
      val response = wait(client.createIndex(name = "foo2"))

      Then("the status code should be 200 OK")
      info(s"ResponseBody: ${response.getResponseBody}")
      response.getStatusCode should be >= 200
      response.getStatusCode should be < 300
    }
  }

  feature("Ability to verify an index exists") {
    scenario("verify an index called 'foo2' exists") {
      When("an attempt to verify the index exists")
      val response = wait(client.verifyIndex("foo2"))

      Then("the status code should be 200 OK")
      info(s"ResponseBody: ${response.getResponseBody}")
      response.getStatusCode should be >= 200
      response.getStatusCode should be < 300
    }
  }

  feature("Ability to create documents within an index") {
    scenario("Create multiple documents in index 'foo2'") {
      When("a document is added to the index")
      val response = wait(client.index(
        index = "foo2", `type` = "foo2", id = Some("foo2"),
        data = "{\"foo2\":\"bar\"}", refresh = true
      ))

      Then("the status code should be 200 OK")
      info(s"ResponseBody: ${response.getResponseBody}")
      response.getStatusCode should be >= 200
      response.getStatusCode should be < 300
    }
  }

  feature("Ability to search for documents") {
    scenario("Fetch a document by it's id") {
      When("attempting to fetch document by it's id")
      val response = wait(client.get("foo2", "foo2", "foo2"))

      Then("the status code should be 200 OK")
      info(s"ResponseBody: ${response.getResponseBody}")
      response.getStatusCode should be >= 200
      response.getStatusCode should be < 300
    }

    scenario("Search for all documents") {
      When("searching for all documents")
      val response = wait(client.search(index = "foo2", query = "{\"query\": { \"match_all\": {} }"))

      Then("the status code should be 200 OK")
      info(s"ResponseBody: ${response.getResponseBody}")
      response.getStatusCode should be >= 200
      response.getStatusCode should be < 300
    }

    scenario("Validate a query") {
      When("validating a query")
      val response = wait(client.validate(index = "foo2", query = "{\"query\": { \"match_all\": {} }"))

      Then("the status code should be 200 OK")
      info(s"ResponseBody: ${response.getResponseBody}")
      response.getStatusCode should be >= 200
      response.getStatusCode should be < 300
    }

    scenario("Explain a query") {
      When("a explain request is made for query")
      val response = wait(client.explain(index = "foo2", `type` = "foo2", id = "foo22", query = "{\"query\": { \"term\": { \"foo2\":\"bar\"} } }"))

      Then("the status code should be 200 OK")
      info(s"ResponseBody: ${response.getResponseBody}")
      response.getStatusCode should be >= 200
      response.getStatusCode should be < 300
    }

  }

  feature("Ability to delete documents and indices") {
    scenario("Delete document 'foo2' directly") {
      When("deleting the document by ID")
      val response = wait(client.delete("foo2", "foo2", "foo2"))

      Then("the status code should be 200 OK")
      info(s"ResponseBody: ${response.getResponseBody}")
      response.getStatusCode should be >= 200
      response.getStatusCode should be < 300
    }

    /*
    scenario("Delete document 'foo2' by query") {
      When("deleting the document by query")
      val response = wait(client.deleteByQuery(Seq("foo2"), Seq.empty[String], "{ \"match_all\": {} }"))

      Then("the status code should be 200 OK")
      info(s"ResponseBody: ${response.getResponseBody}")
      response.getStatusCode should be >= 200
      response.getStatusCode should be < 300
    } */

    scenario("Count the matches to a query") {
      When("counting the matches to a query")
      val response = wait(client.count(Seq("foo2"), Seq("foo2"), "{\"query\": { \"match_all\": {} }"))

      Then("the status code should be 200 OK")
      info(s"ResponseBody: ${response.getResponseBody}")
      response.getStatusCode should be >= 200
      response.getStatusCode should be < 300
    }

    scenario("Delete index 'foo2'") {
      When("deleting index 'foo2'")
      val response = wait(client.deleteIndex("foo2"))

      Then("the status code should be 200 OK")
      info(s"ResponseBody: ${response.getResponseBody}")
      response.getStatusCode should be >= 200
      response.getStatusCode should be < 300
    }
  }

  def wait[A](task: Future[A]): A = Await.result(task, 15.seconds)
  */
}
