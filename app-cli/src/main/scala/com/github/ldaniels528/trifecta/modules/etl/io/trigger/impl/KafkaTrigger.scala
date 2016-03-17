package com.github.ldaniels528.trifecta.modules.etl.io.trigger.impl

import com.github.ldaniels528.trifecta.modules.etl.StoryConfig
import com.github.ldaniels528.trifecta.modules.etl.io.Scope
import com.github.ldaniels528.trifecta.modules.etl.io.device.OutputSource
import com.github.ldaniels528.trifecta.modules.etl.io.trigger.Trigger
import kafka.consumer.{Consumer, ConsumerConfig}

import scala.concurrent.{ExecutionContext, Future, blocking}

/**
  * Kafka Trigger
  * @author lawrence.daniels@gmail.com
  */
case class KafkaTrigger(id: String,
                        topic: String,
                        parallelism: Int,
                        consumerConfig: ConsumerConfig,
                        output: OutputSource)
  extends Trigger {

  private val consumer = Consumer.create(consumerConfig)

  override def destroy() = {}

  override def execute(config: StoryConfig)(implicit ec: ExecutionContext) = {
    val streamMap = consumer.createMessageStreams(Map(topic -> parallelism))

    for {
      streams <- streamMap.get(topic)
      stream <- streams
    } {
      Future(blocking {
        implicit val scope = Scope()
        val it = stream.iterator()
        while (it.hasNext()) {
          val mam = it.next()
          // TODO add replacement logic here
          /*
          val dataSet = Seq(Data(fieldSet, mam.message()))
          layout.out(scope, output, dataSet, isEOF = false) foreach { outdata =>
            output.write(scope, outdata)
          }*/
        }
      })
    }

    logger.info("Done")
  }

}

