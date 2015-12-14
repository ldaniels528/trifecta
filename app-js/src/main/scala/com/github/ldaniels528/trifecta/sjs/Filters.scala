package com.github.ldaniels528.trifecta.sjs

import com.github.ldaniels528.scalascript._
import com.github.ldaniels528.scalascript.util.ScalaJsHelper._

import scala.scalajs.js

/**
  * Trifecta AngularJS Filters
  * @author lawrence.daniels@gmail.com
  */
object Filters {
  private val timeUnits = Seq("min", "hour", "day", "month", "year")
  private val timeFactors = Seq(60, 24, 30, 12)

  /**
    * Capitalize: Returns the capitalize representation of a given string
    */
  val capitalize: js.Function = () => { (value: js.UndefOr[String]) =>
    value map { s => if (s.nonEmpty) s.head.toUpper + s.tail else "" }
  }: js.Function

  /**
    * Duration: Converts a given time stamp to a more human readable expression (e.g. "5 mins ago")
    */
  val duration: js.Function = () => { (time: js.Dynamic) => toDuration(time, noFuture = false) }: js.Function

  /**
    * Yes/No: Converts a boolean value into 'Yes' or 'No'
    */
  val yesNo: js.Function = () => ((state: Boolean) => if (state) "Yes" else "No"): js.Function

  /**
    * Converts the given time expression to a textual duration
    * @param time the given time stamp (in milliseconds)
    * @return the duration (e.g. "10 mins ago")
    */
  private def toDuration(time: js.UndefOr[js.Any], noFuture: Boolean = false) = {
    // get the time in milliseconds
    val myTime = time.toOption map {
      case value if angular.isDate(value) => value.asInstanceOf[js.Date].getTime()
      case value if angular.isNumber(value) => value.asInstanceOf[Double]
      case value if angular.isObject(value) =>
        val obj = value.asInstanceOf[js.Dynamic]
        if (angular.isDefined(obj.$date)) obj.$date.asOpt[Double].getOrElse(js.Date.now())
        else js.Date.now()
      case _ => js.Date.now()
    } getOrElse js.Date.now()

    // compute the elapsed time
    val elapsed = (js.Date.now() - myTime) / 60000

    // compute the age
    var age = Math.abs(elapsed)
    var unit = 0
    while (unit < timeFactors.length && age >= timeFactors(unit)) {
      age /= timeFactors(unit)
      unit += 1
    }

    // make the age and unit names more readable
    val unitName = timeUnits(unit) + (if (age.toInt != 1) "s" else "")
    if (unit == 0 && (age >= 0 && age < 1)) "just now"
    else if (elapsed < 0) {
      if (noFuture) "moments ago" else f"$age%.0f $unitName from now"
    }
    else f"$age%.0f $unitName ago"
  }

}
