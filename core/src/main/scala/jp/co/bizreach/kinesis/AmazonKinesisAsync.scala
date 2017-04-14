package jp.co.bizreach.kinesis

import com.amazonaws.ClientConfiguration
import com.amazonaws.auth.AWSCredentialsProvider
import com.amazonaws.handlers.AsyncHandler
import com.amazonaws.regions.Regions
import com.amazonaws.services.kinesis.{AmazonKinesisAsync => AWSKinesisAsync, AmazonKinesisAsyncClientBuilder}
import com.amazonaws.services.kinesis.model.{
  PutRecordRequest => AWSPutRecordRequest,
  PutRecordResult => AWSPutRecordResult,
  PutRecordsRequest => AWSPutRecordsRequest,
  PutRecordsResult => AWSPutRecordsResult}

import scala.concurrent._

object AmazonKinesisAsync {
  def apply()(implicit region: Regions): AmazonKinesisAsync = {
    new AmazonKinesisAsync(AmazonKinesisAsyncClientBuilder.standard
      .withRegion(region)
      .build())
  }
  def apply(credentials: AWSCredentialsProvider)(implicit region: Regions): AmazonKinesisAsync = {
    new AmazonKinesisAsync(AmazonKinesisAsyncClientBuilder.standard
      .withCredentials(credentials)
      .withRegion(region)
      .build())
  }
  def apply(config: ClientConfiguration)(implicit region: Regions): AmazonKinesisAsync = {
    new AmazonKinesisAsync(AmazonKinesisAsyncClientBuilder.standard
      .withClientConfiguration(config)
      .withRegion(region)
      .build())
  }
  def apply(credentials: AWSCredentialsProvider, config: ClientConfiguration)(implicit region: Regions): AmazonKinesisAsync = {
    new AmazonKinesisAsync(AmazonKinesisAsyncClientBuilder.standard
      .withCredentials(credentials)
      .withClientConfiguration(config)
      .withRegion(region)
      .build())
  }
  def apply(client: AWSKinesisAsync): AmazonKinesisAsync = {
    new AmazonKinesisAsync(client)
  }
}

/**
 * Trial implementation of AmazonKinesisAsync for Scala.
 */
class AmazonKinesisAsync(client: AWSKinesisAsync) {

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
