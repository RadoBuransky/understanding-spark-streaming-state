package com.buransky.understandingSparkStreamingState

import net.manub.embeddedkafka.EmbeddedKafka._

/**
  * Application #2.
  *
  * To understand everything run it twice. First run with arguments "4 1" and second run twith "6 2".
  */
object App2Recover extends BaseApp {
  override def main(args: Array[String]): Unit = {
    BaseApp.failOn = ""
    withRunningKafka {
      val prefix = if (args.length < 3) "" else args(2)
      publishMessagesToKafka(args(0).toInt, args(1).toInt, stopAfterLastMessage = true, prefix)

      withSsc() { inputStream =>
        inputStream.mapWithState(BaseApp.stringStateSpec)
      }
    }
  }
}