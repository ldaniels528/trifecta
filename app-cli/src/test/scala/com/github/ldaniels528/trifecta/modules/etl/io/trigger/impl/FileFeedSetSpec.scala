package com.github.ldaniels528.trifecta.modules.etl.io.trigger.impl

import java.io.File

import com.github.ldaniels528.trifecta.modules.etl.io.trigger.impl.FileFeedSet.FeedMatch
import org.scalatest.Matchers._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{BeforeAndAfterEach, FeatureSpec, GivenWhenThen}

/**
  * File Feed Set Specification
  * @author lawrence.daniels@gmail.com
  */
class FileFeedSetSpec() extends FeatureSpec with BeforeAndAfterEach with GivenWhenThen with MockitoSugar {

  info("As a FileFeedSet instance")
  info("I want to be able to process files in sets")

  feature("Identify similar files") {
    scenario("Identify files with the same patterns") {
      Given("a feed set")
      val feedSet = FileFeedSet(
        path = "{{ user.home }}/broadway/incoming/tradingHistory",
        pattern = ".*_(\\S*)[.]txt",
        archive = None,
        feeds = Seq(
          FileFeed.startsWith("AMEX", flows = Nil, archive = None),
          FileFeed.startsWith("NASDAQ", flows = Nil, archive = None),
          FileFeed.startsWith("NYSE", flows = Nil, archive = None),
          FileFeed.startsWith("OTCBB", flows = Nil, archive = None)
        ))

      And("a set of files")
      val files = Seq(
        new File("AMEX_20160101.txt"),
        new File("NASDAQ_20160101.txt"),
        new File("NYSE_20160101.txt"),
        new File("OTCBB_20160101.txt")
      )

      When("the pattern is tested")
      feedSet.isSatisfied(files) shouldBe true

      Then("a match should be found")
      val results = feedSet.getFiles(files)
      results foreach { case FeedMatch(groupId, file, feed) =>
        info(s"groupId: $groupId, file: $file, feed: $feed")
      }
      val expected = feedSet.feeds zip files map { case (feed, file) =>
        FeedMatch(groupId = "20160101", file, feed)
      }
      expected shouldBe results
    }
  }

}
