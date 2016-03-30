package jp.co.bizreach.kinesis

import com.amazonaws.ClientConfiguration
import com.amazonaws.auth.AWSCredentialsProvider
import com.amazonaws.handlers.AsyncHandler
import com.amazonaws.regions.Regions
import com.amazonaws.services.kinesis.{AmazonKinesisAsyncClient => AWSKinesisAsyncClient}
import com.amazonaws.services.kinesis.model.{
  PutRecordRequest => AWSPutRecordRequest,
  PutRecordResult => AWSPutRecordResult,
  PutRecordsRequest => AWSPutRecordsRequest,
  PutRecordsResult => AWSPutRecordsResult}

import scala.concurrent._

object AmazonKinesisAsyncClient {
  def apply()(implicit region: Regions): AmazonKinesisAsyncClient = {
    new AmazonKinesisAsyncClient(new AWSKinesisAsyncClient().withRegion(region))
  }
  def apply(credentials: AWSCredentialsProvider)(implicit region: Regions): AmazonKinesisAsyncClient = {
    new AmazonKinesisAsyncClient(new AWSKinesisAsyncClient(credentials).withRegion(region))
  }
  def apply(config: ClientConfiguration)(implicit region: Regions): AmazonKinesisAsyncClient = {
    new AmazonKinesisAsyncClient(new AWSKinesisAsyncClient(config).withRegion(region))
  }
  def apply(credentials: AWSCredentialsProvider, config: ClientConfiguration)(implicit region: Regions): AmazonKinesisAsyncClient = {
    new AmazonKinesisAsyncClient(new AWSKinesisAsyncClient(credentials, config).withRegion(region))
  }
  def apply(client: AWSKinesisAsyncClient): AmazonKinesisAsyncClient = {
    new AmazonKinesisAsyncClient(client)
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
