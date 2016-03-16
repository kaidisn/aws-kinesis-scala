package jp.co.bizreach.kinesis

import com.amazonaws.ClientConfiguration
import com.amazonaws.auth.AWSCredentialsProvider
import com.amazonaws.regions.Regions
import com.amazonaws.services.kinesis.{AmazonKinesisClient => AWSKinesisClient}
import jp.co.bizreach.kinesis.action.PutRecordAction

object AmazonKinesisClient {
  def apply()(implicit region: Regions): AmazonKinesisClient = {
    new AmazonKinesisClient(new AWSKinesisClient().withRegion(region)) with PutRecordAction
  }
  def apply(credentials: AWSCredentialsProvider)(implicit region: Regions): AmazonKinesisClient = {
    new AmazonKinesisClient(new AWSKinesisClient(credentials).withRegion(region)) with PutRecordAction
  }
  def apply(config: ClientConfiguration)(implicit region: Regions): AmazonKinesisClient = {
    new AmazonKinesisClient(new AWSKinesisClient(config).withRegion(region)) with PutRecordAction
  }
  def apply(credentials: AWSCredentialsProvider, config: ClientConfiguration)(implicit region: Regions): AmazonKinesisClient = {
    new AmazonKinesisClient(new AWSKinesisClient(credentials, config).withRegion(region)) with PutRecordAction
  }
}

class AmazonKinesisClient(client: AWSKinesisClient) {
  self: PutRecordAction =>

  def putRecord(request: PutRecordRequest): PutRecordResult = {
    client.putRecord(request)
  }

  def putRecordWithRetry(request: PutRecordRequest): Either[Throwable, PutRecordResult] = {
    withRetry(0){
      client.putRecord(request)
    }
  }

  def putRecords(request: PutRecordsRequest): PutRecordsResult = {
    client.putRecords(request)
  }

  def putRecordsWithRetry(request: PutRecordsRequest): Either[Seq[(PutRecordsEntry, PutRecordsResultEntry)], Unit] = {
    withRetry(request.records){ entry =>
      client.putRecords(PutRecordsRequest(request.streamName, entry))
    }
  }

  def shutdown(): Unit = {
    client.shutdown()
  }
}
