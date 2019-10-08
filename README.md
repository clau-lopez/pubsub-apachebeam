# Apache Beam Example

## Compile & Package

```
mvn clean package
```

## Environment Variables

```
export GOOGLE_APPLICATION_CREDENTIALS=path/to/my/credentials.json
export BUCKET_NAME=BUCKET
export PROJECT_NAME=PROJECT
```

## Run
```
mvn compile exec:java \\
  -Dexec.mainClass=com.examples.pubsub.streaming.PubSubToGCS \\
  -Dexec.cleanupDaemonThreads=false \\
  -Dexec.args="\\
    --project=$PROJECT_NAME \\
    --inputTopic=projects/$PROJECT_NAME/topics/my-topic \\
    --output=gs://$BUCKET_NAME/apache-beam-example/output \\
    --runner=DataflowRunner \\
    --windowSize=2"
```
##