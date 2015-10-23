aws-kinesis-scala
========

Scala client for Amazon Kinesis

## How to use

Add a following dependency into your `build.sbt` at first.

```scala
libraryDependencies += "jp.co.bizreach" %% "aws-kinesis-scala" % "0.0.1"
```

Then you can access Kinesis as following:

```scala
import jp.co.bizreach.kinesis._
import com.amazonaws.auth.ClasspathPropertiesFileCredentialsProvider

val credentialsProvider = new ClasspathPropertiesFileCredentialsProvider()
val kinesisClient = AmazonKinesisClient(credentialsProvider)

val putRecordRequest = PutRecordRequest(
  streamName   = "streamName",
  partitionKey = "partitionKey",
  data         = "data".getBytes("UTF-8")
)

kinesisClient.putRecord(putRecordRequest)
```
