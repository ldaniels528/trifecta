package com.github.ldaniels528.trifecta.io.avro

import com.github.ldaniels528.trifecta.io.avro.AvroConversionSpec.{StudentA, StudentB, StudentBean}
import com.github.ldaniels528.tabular.Tabular
import org.scalatest.Matchers._
import org.scalatest.{FeatureSpec, GivenWhenThen}

import scala.beans.BeanProperty

/**
 * Avro Conversion Utility Specification
 * @author lawrence.daniels@gmail.com
 */
class AvroConversionSpec() extends FeatureSpec with GivenWhenThen {
  private val tabular = new Tabular()

  info("As an Avro Conversion Utility")
  info("I want to be able to copy data from case classes to Java Beans")

  feature("Ability to copy data from a Scala case class to a Java Bean") {
    scenario("Copy values from a Scala case class to a Java Bean instance") {
      Given("A case class and a Java Bean instance")
      val student = StudentA("1502460", "Joseph Smith", 19)
      tabular.transform(Seq(student)) foreach (info(_))
      val builder = new StudentBean()

      When("The copy method is executed")
      AvroConversion.copy(student, builder)

      Then("The Java Bean should contain the values from the case class")
      student.id shouldBe builder.getId
      student.name shouldBe builder.getName
      student.age shouldBe builder.getAge
    }
  }

  feature("Ability to copy data from a case class with Option types to a Java Bean") {
    scenario("Copy values from a Scala case class with Option types to a Java Bean instance") {
      Given("A case class and a Java Bean instance")
      val student = StudentB(Option("1502460"), Option("Joseph Smith"), Option(19))
      tabular.transform(Seq(student)) foreach (info(_))
      val builder = new StudentBean()

      When("The copy method is executed")
      AvroConversion.copy(student, builder)

      Then("The Java Bean should contain the values from the case class")
      student.id.get shouldBe builder.getId
      student.name.get shouldBe builder.getName
      student.age.get shouldBe builder.getAge
    }
  }

}

object AvroConversionSpec {

  case class StudentA(id: String, name: String, age: Int)

  case class StudentB(id: Option[String], name: Option[String], age: Option[Int])

  class StudentBean() {
    @BeanProperty var id: String = _
    @BeanProperty var name: String = _
    @BeanProperty var age: Integer = _
  }

}
