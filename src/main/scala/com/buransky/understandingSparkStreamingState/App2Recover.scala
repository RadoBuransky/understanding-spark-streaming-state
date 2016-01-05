package com.buransky.understandingSparkStreamingState

/**
  * Application #2.
  *
  * Run App1PublishAndFail before running this one. I would assume that this application uses checkpoint created by
  * the App1PublishAndFail, but I am obviously wrong. I expected the "c" message to be redelivered, but nothing happens.
  */
object App2Recover extends BaseApp {
  override def main(args: Array[String]): Unit = {
    BaseApp.failOn = ""
    withKafkaAndSsc() { inputStream =>
      inputStream.mapWithState(BaseApp.stringStateSpec)
    }
  }
}