package com.github.ldaniels528.trifecta.modules.etl.io.flow.impl

import com.github.ldaniels528.trifecta.modules.etl.io.Scope
import com.github.ldaniels528.trifecta.modules.etl.io.device.{AsynchronousOutputSupport, InputSource, OutputSource}
import com.github.ldaniels528.trifecta.modules.etl.io.flow.Flow
import com.github.ldaniels528.trifecta.modules.etl.io.layout.Layout.InputSet
import com.github.ldaniels528.commons.helpers.OptionHelper.Risky._

import scala.concurrent.{ExecutionContext, Future}

/**
  * Composite Input Flow
  * @author lawrence.daniels@gmail.com
  */
case class CompositeFlow(id: String, inputs: Seq[InputSource], outputs: Seq[OutputSource]) extends Flow {

  override def devices = (outputs ++ inputs).sortBy(_.id)

  override def execute(scope: Scope)(implicit ec: ExecutionContext) = {
    implicit val myScope = scope

    // open the output sources, which will remain open throughout the process
    outputs foreach (_.open)

    // cycle through the input sources
    inputs foreach (_ use { input =>
      var inputSet: Option[InputSet] = None
      do {
        // read the input record(s)
        inputSet = input.layout.read(input)

        // transform the output record(s)
        inputSet.foreach(inputSet => outputs.foreach(output => output.layout.write(output, inputSet)))

      } while (inputSet.exists(!_.isEOF))
    })

    // ask to be notified once all asynchronous writes have completed
    val task = Future.sequence((outputs map {
      case aos: AsynchronousOutputSupport => aos.allWritesCompleted
      case _ => Future.successful(outputs)
    }).toList) map(_.flatten)

    // close the output source once all writes have completed
    task onComplete (_ => outputs.foreach(_.close))
    task.map(_ => ())
  }

}