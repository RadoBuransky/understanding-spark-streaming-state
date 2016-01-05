package com.buransky.understandingSparkStreamingState

/**
  * Application #1.
  *
  * Before running delete local ./checkpoints folder.
  *
  * This application should publish four messages to Kafka "a", "b", "c", "d" and should simulate JVM failure while
  * processing the "c" message. So if the application brutally crashes that's what we want.
  *
  * The application will now have a checkpoint from which it should be able to recover. Take a look into the
  * ./checkpoints folder to see if there is one subdirectory. Proceed to the application #2 App2Recover.
  */
object App1PublishAndFail extends BaseApp {
  override def main(args: Array[String]): Unit = {
    withSsc(failOn = "c") { inputStream =>
      inputStream.mapWithState(BaseApp.stringStateSpec)
    }
  }
}