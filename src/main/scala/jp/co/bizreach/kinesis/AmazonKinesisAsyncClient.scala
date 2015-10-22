package jp.co.bizreach.kinesis

import com.amazonaws.services.kinesis.model.{PutRecordRequest => AWSPutRecordRequest}
import com.amazonaws.services.kinesis.model.{PutRecordResult => AWSPutRecordResult}
import com.amazonaws.services.kinesis.model.{PutRecordsRequest => AWSPutRecordsRequest}
import com.amazonaws.services.kinesis.model.{PutRecordsResult => AWSPutRecordsResult}

import com.amazonaws.services.kinesis.{AmazonKinesisAsyncClient => AWSKinesisAsyncClient}

import com.amazonaws.auth.AWSCredentialsProvider
import com.amazonaws.ClientConfiguration
import com.amazonaws.handlers.AsyncHandler
import scala.concurrent._

object AmazonKinesisAsyncClient {
  def apply(): AmazonKinesisAsyncClient = {
    new AmazonKinesisAsyncClient(new AWSKinesisAsyncClient())
  }
  def apply(awsCredentialsProvider: AWSCredentialsProvider): AmazonKinesisAsyncClient = {
    new AmazonKinesisAsyncClient(new AWSKinesisAsyncClient(awsCredentialsProvider))
  }
  def apply(clientConfiguration: ClientConfiguration): AmazonKinesisAsyncClient = {
    new AmazonKinesisAsyncClient(new AWSKinesisAsyncClient(clientConfiguration))
  }
  def apply(awsCredentialsProvider: AWSCredentialsProvider, clientConfiguration: ClientConfiguration): AmazonKinesisAsyncClient = {
    new AmazonKinesisAsyncClient(new AWSKinesisAsyncClient(awsCredentialsProvider, clientConfiguration))
  }
}

/**
 * Trial implementation of AmazonKinesisAsyncClient for Scala. 
 */
class AmazonKinesisAsyncClient(client: AWSKinesisAsyncClient) {

  def putRecordAsync(request: PutRecordRequest): Future[PutRecordResult] = {
    val p = Promise[PutRecordResult]
    client.putRecordAsync(request, new AsyncHandler[AWSPutRecordRequest, AWSPutRecordResult]{
      override def onError(e: Exception): Unit = p.failure(e)
      override def onSuccess(req: AWSPutRecordRequest, res: AWSPutRecordResult): Unit = p.success(res)
    })
    p.future
  }

  def putRecordsAsync(request: PutRecordsRequest): Future[PutRecordsResult] = {
    val p = Promise[PutRecordsResult]
    client.putRecordsAsync(request, new AsyncHandler[AWSPutRecordsRequest, AWSPutRecordsResult]{
      override def onError(e: Exception): Unit = p.failure(e)
      override def onSuccess(req: AWSPutRecordsRequest, res: AWSPutRecordsResult): Unit = p.success(res)
    })
    p.future
  }

  def shutdown(): Unit = {
    client.shutdown()
  }
}
