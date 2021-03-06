package com.buransky.understandingSparkStreamingState

import java.util.concurrent.atomic.AtomicBoolean

import _root_.kafka.serializer.StringDecoder
import com.buransky.understandingSparkStreamingState.BaseApp._
import net.manub.embeddedkafka.EmbeddedKafka
import net.manub.embeddedkafka.EmbeddedKafka._
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.slf4j.LoggerFactory

import scala.concurrent._

trait BaseApp {
  def main(args: Array[String]): Unit

  def withKafkaAndSsc()(action: (DStream[(String, String)]) => DStream[(String, String)]): Unit = {
    withRunningKafka {
      // Create topic otherwise creation will crash
      publishStringMessageToKafka(kafkaTopic, "a")
      publishStringMessageToKafka(kafkaTopic, "b")
      publishStringMessageToKafka(kafkaTopic, "c")
      publishStringMessageToKafka(kafkaTopic, "d")

      withSsc()(action)
    }
  }

  def withSsc()(action: (DStream[(String, String)]) => DStream[(String, String)]): Unit = {
    val ssc = StreamingContext.getOrCreate(checkpointDir, () => createSsc(action))

    if (lastMessage.nonEmpty) {
      Future {
        blocking {
          while (!BaseApp.lastMessageProcessesed.get()) {
            Thread.sleep(100)
          }
          log.debug("Stopping SSC after last message...")
          EmbeddedKafka.stop()
          ssc.stop(stopSparkContext = true, stopGracefully = true)
          log.debug("SSC stopped after last message.")
        }
      } (ExecutionContext.global)
    }

    ssc.start()

    log.debug("Awaiting termination...")
    ssc.awaitTermination()
    log.debug("SSC terminated.")
  }

  protected def publishMessagesToKafka(count: Int,
                                       repeats: Int = 1,
                                       stopAfterLastMessage: Boolean = true,
                                       prefix: String = ""): Unit = {
    def msg(r: Int, c: Int) = s"$prefix$r:$c"
    for (i <- 1 to repeats) {
      for (j <- 1 to count) {
        publishStringMessageToKafka(kafkaTopic, msg(i, j))
      }
    }
    if (stopAfterLastMessage)
      BaseApp.lastMessage = msg(repeats, count)
  }

  private def createSsc(action: (DStream[(String, String)]) => DStream[(String, String)]): StreamingContext = {
    log.debug(s"Creating SSC. [$failOn, $murder]")

    // Create Spark configuration
    val conf = new SparkConf().setAppName(appName)
      .setMaster("local[2]")

    val ssc = new StreamingContext(conf, Seconds(1))
    ssc.checkpoint("./checkpoints")

    // Connect to embedded Kafka
    val kafkaStream = createKafkaStream(ssc).map { m =>
      val kv = m._2.split(":")
      kv(1) -> m._2
    }

    // Invoke action and print it
    action(kafkaStream).foreachRDD { rdd =>
      rdd.foreach { case (k, v) =>
        if (k == failOn) {
          if (murder)
            Runtime.getRuntime.halt(-1)
          else
            throw new RuntimeException("Fail!")
        }
        if (v == lastMessage) {
          log.debug(s"Last message received. [$v]")
          lastMessageProcessesed.set(true)
        }
        else {
          log.debug(s"Message received. [$v]")
        }
      }
    }

    ssc
  }

  private def createKafkaStream(ssc: StreamingContext): DStream[(String, String)] = {
    // Configure Kafka
    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> "localhost:6001",
      "auto.offset.reset" -> "smallest"
    )

    log.debug(s"Kafka params. [$kafkaParams]")

    // Create direct Kafka stream
    KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, Set(kafkaTopic))
  }
}

object BaseApp {
  val log = LoggerFactory.getLogger("com.buransky.App")

  var failOn: String = ""
  var murder: Boolean = false

  var lastMessage: String = ""
  val lastMessageProcessesed: AtomicBoolean = new AtomicBoolean(false)

  val checkpointDir = "./checkpoints"
  val appName = "UnderstandingSparkStreamingState"
  val kafkaTopic = "test"

  lazy val stringStateSpec = StateSpec.function[String, String, String, (String, String)](stateMapping _)

  private def stateMapping(time: Time,
                           key: String,
                           value: Option[String],
                           oldState: State[String]): Option[(String, String)] = {
    val newState = value.toString
    log.debug(s"State mapping: [key: $key, value: $value, $oldState -> $newState]")
    oldState.update(newState)
    value.map(key -> _)
  }
}