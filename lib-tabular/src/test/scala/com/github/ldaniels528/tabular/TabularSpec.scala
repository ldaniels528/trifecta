package com.github.ldaniels528.tabular

import com.github.ldaniels528.tabular.formatters.NumberFormatHandler
import org.scalatest.Matchers._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{FeatureSpec, GivenWhenThen}

/**
 * Tabular Specification
 * @author lawrence.daniels@gmail.com
 */
class TabularSpec() extends FeatureSpec with GivenWhenThen with MockitoSugar {

  info("As a Tabular instance")
  info("I want to be able to transform collections of case classes into an ASCII grid of rows and columns")

  /**
   * Generates a basic table from a case class.
   *
   * Expected output:
   * + ------------------------------- +
   * | item    quantity  requestedBy   |
   * + ------------------------------- +
   * | Milk    1         kids          |
   * | Eggs    1         Mom           |
   * | Cheese  1         Mom           |
   * | Beer    12        Dad           |
   * + ------------------------------- +
   */

  feature("Ability to represent a table of data using ASCII characters") {
    scenario("Transform collections of case classes into a string array representing a table") {
      Given("A Tabular instance")
      val tabular = new Tabular()

      And("A collections of case class instances representing the data")
      val groceryList = Seq(
        GroceryItem("Milk", requestedBy = "kids"),
        GroceryItem("Eggs", requestedBy = "Mom"),
        GroceryItem("Cheese", requestedBy = "Mom"),
        GroceryItem("Beer", quantity = 12, requestedBy = "Dad"))
      groceryList foreach (s => info(s.toString))
      info("")

      When("Tabular is invoked")
      val result = tabular.transform(groceryList)

      Then("The result size should be 8")
      result.size shouldBe 8

      And("The the output should match the expected result")
      val lines = "+ ------------------------------- +\n| item    quantity  requestedBy   |\n+ ------------------------------- +\n| Milk    1         kids          |\n| Eggs    1         Mom           |\n| Cheese  1         Mom           |\n| Beer    12        Dad           |\n+ ------------------------------- +".split("\n")
      result foreach (s => info(s))
      result shouldBe lines
    }
  }

  /**
   * Generates a basic table from a case class.
   *
   * Expected output:
   * + ----------------- +
   * | name      score   |
   * + ----------------- +
   * | Lawrence  5,000   |
   * | Stan      6,500   |
   * | Cullen    5,700   |
   * + ----------------- +
   */

  feature("Ability to represent a table of data using ASCII characters with custom numeric transforms") {
    scenario("Transform collections of case classes into a string array representing a table") {
      Given("A Tabular instance with number formatting mixed-in")
      val tabular = new Tabular() with NumberFormatHandler

      And("A collections of case class instances representing the data:")
      val scores = Seq(
        Score("Lawrence", 5000),
        Score("Stan", 6500),
        Score("Cullen", 5700))
      scores foreach (s => info(s.toString))
      info("")

      When("Tabular is invoked")
      val result = tabular.transform(scores)

      Then("The result size should be 7")
      result.size shouldBe 7

      And("The the output should match the expected result")
      val lines = "+ ----------------- +\n| name      score   |\n+ ----------------- +\n| Lawrence  5,000   |\n| Stan      6,500   |\n| Cullen    5,700   |\n+ ----------------- +".split("\n")
      result foreach (s => info(s))
      result shouldBe lines
    }
  }

  case class GroceryItem(item: String, quantity: Int = 1, requestedBy: String)

  case class Score(name: String, score: Int)

}
