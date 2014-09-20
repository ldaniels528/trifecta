package com.ldaniels528.verify.support.avro

import com.ldaniels528.tabular.Tabular
import com.ldaniels528.verify.support.avro.AvroConversionSpec.{Student, StudentBuilder}
import org.scalatest.Matchers._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{FeatureSpec, GivenWhenThen}

import scala.beans.BeanProperty

/**
 * Avro Conversion Utility Specification
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class AvroConversionSpec() extends FeatureSpec with GivenWhenThen with MockitoSugar {
  private val tabular = new Tabular()

  info("As an Avro Conversion Utility")
  info("I want to be able to copy data from case classes to Avro Builders")

  feature("Ability to copy data from a case class to an Avro Builder") {
    scenario("Compress then decompress a message") {
      Given("A case class and an Avro builder instance")
      val student = Student("1502460", "Joseph Smith", 19)
      tabular.transform(Seq(student)) foreach (info(_))
      val builder = new StudentBuilder()

      When("The copy method is executed")
      AvroConversion.copy(student, builder)

      Then("The builder should contain the values from the case class")
      student.id shouldBe builder.getId
      student.name shouldBe builder.getName
      student.age shouldBe builder.getAge
    }
  }

}

object AvroConversionSpec {

  case class Student(id: String, name: String, age: Int)

  class StudentBuilder() {
    @BeanProperty var id: String = _
    @BeanProperty var name: String = _
    @BeanProperty var age: Integer = _
  }

}
