# Understanding Spark Streaming state

## App1PublishAndFail

Application #1.
  
Before running delete local `./checkpoints` folder.

This application should publish four messages to Kafka "a", "b", "c", "d" and should simulate JVM failure while
processing the "c" message. So if the application brutally crashes that's what we want.

The application will now have a checkpoint from which it should be able to recover. Take a look into the
`./checkpoints` folder to see if there is one subdirectory. Proceed to the application #2 App2Recover.

## App2Recover

Application #2.

Run App1PublishAndFail before running this one. I would assume that this application uses checkpoint created by
the App1PublishAndFail, but I am obviously wrong. I expected the "c" message to be redelivered, but nothing happens.