package com.buransky.understandingSparkStreamingState

import _root_.kafka.serializer.StringDecoder
import com.buransky.understandingSparkStreamingState.BaseApp._
import net.manub.embeddedkafka.EmbeddedKafka._
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.slf4j.LoggerFactory

trait BaseApp {
  def main(args: Array[String]): Unit

  def withSsc(failOn: String)(action: (DStream[(String, String)]) => DStream[(String, String)]): Unit = {
    withRunningKafka {
      // Create Spark configuration
      val conf = new SparkConf().setAppName(appName)
        .setMaster("local[2]")

      val ssc = new StreamingContext(conf, Seconds(1))
      ssc.checkpoint("./checkpoints")

      publishStringMessageToKafka(kafkaTopic, "")

      // Connect to embedded Kafka
      val kafkaStream = createKafkaStream(ssc).map(m => m._2 -> m._2)

      if (failOn.nonEmpty) {
        publishStringMessageToKafka(kafkaTopic, "a")
        publishStringMessageToKafka(kafkaTopic, "b")
        publishStringMessageToKafka(kafkaTopic, "c")
        publishStringMessageToKafka(kafkaTopic, "d")
      }

      // Invoke action and print it
      action(kafkaStream).foreachRDD { rdd =>
        rdd.foreach { case (k, v) =>
          if (k == failOn)
            Runtime.getRuntime.halt(-1)
          log.debug(s"Message received. [$v]")
        }
      }

      ssc.start()
      if (failOn.isEmpty) {
        ssc.awaitTermination()
        ssc.stop(stopSparkContext = true, stopGracefully = true)
      }
      else {
        log.debug("Awaiting termination...")
        ssc.awaitTermination()
      }
    }
  }

  private def createKafkaStream(ssc: StreamingContext): DStream[(String, String)] = {
    // Configure Kafka
    val kafkaParams = Map[String, String]("metadata.broker.list" -> "localhost:6001")

    // Create direct Kafka stream
    KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, Set(kafkaTopic))
  }
}

object BaseApp {
  val log = LoggerFactory.getLogger("com.buransky.App")

  val appName = "UnderstandingSparkStreamingState"
  val kafkaTopic = "test"

  lazy val stringStateSpec = StateSpec.function[String, String, String, (String, String)](stateMapping("abc") _)
  lazy val intStateSpec = StateSpec.function[String, String, Int, (String, String)](stateMapping(42) _)

  private def stateMapping[StateType](fixedState: StateType)(time: Time,
                                                             key: String,
                                                             value: Option[String],
                                                             state: State[StateType]): Option[(String, String)] = {
    log.debug(s"key: $key, value: $value, $state")
    state.update(fixedState)
    value.map(key -> _)
  }
}