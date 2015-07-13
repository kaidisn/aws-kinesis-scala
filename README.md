aws-kinesis-scala
========

Scala client for Amazon Kinesis

## How to use

Add a following dependency into your `build.sbt` at first.

```scala
libraryDependencies += "jp.co.bizreach" %% "aws-kinesis-scala" % "0.0.1-SNAPSHOT"
```

Then you can access Kinesis as following:

```scala
import jp.co.bizreach.kinesis._
import com.amazonaws.auth.ClasspathPropertiesFileCredentialsProvider

val credentialsProvider = new ClasspathPropertiesFileCredentialsProvider()
val kinesisClient = AmazonKinesisClient(credentialsProvider)

val putRecordRequest = new PutRecordRequest(
  streamName   = "spark-test",
  partitionKey = "key1",
  data         = "test".getBytes("UTF-8")
)

kinesisClient.putRecordAsync(putRecordRequest)
```
