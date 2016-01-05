package com.buransky.understandingSparkStreamingState

import net.manub.embeddedkafka.EmbeddedKafka._
import BaseApp._
import org.apache.spark.SparkException

/**
  * Application #3.
  *
  * Before running delete local ./checkpoints folder.
  *
  * This application should do the same as apps #1 and then #2 but in a single JVM process.
  */
object App3PublishFailAndRecover extends BaseApp {
  override def main(args: Array[String]): Unit = {
    withRunningKafka {
      // Create topic otherwise creation will crash
      publishStringMessageToKafka(kafkaTopic, "a")
      publishStringMessageToKafka(kafkaTopic, "b")
      publishStringMessageToKafka(kafkaTopic, "c")
      publishStringMessageToKafka(kafkaTopic, "d")

      // First step is to simulate a failure ...
      BaseApp.failOn = "c"
      BaseApp.murder = false
      try {
        withSsc() { inputStream =>
          inputStream.mapWithState(stringStateSpec)
        }
      }
      catch {
        case ex: SparkException if ex.getCause.getMessage == "Fail!" => // This is expected
        case other =>
          log.error("WTF", other)
          throw other
      }

      // ... second step is to try to recover from it.
      BaseApp.failOn = ""
      withSsc() { inputStream =>
        inputStream.mapWithState(stringStateSpec)
      }
    }
  }
}