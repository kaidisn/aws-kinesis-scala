package jp.co.bizreach.kinesis

import com.amazonaws.ClientConfiguration
import com.amazonaws.auth.AWSCredentialsProvider
import com.amazonaws.services.kinesis.{AmazonKinesisClient => AWSKinesisClient}
import jp.co.bizreach.kinesis.action.PutRecordAction

object AmazonKinesisClient {
  def apply(): AmazonKinesisClient = {
    new AmazonKinesisClient(new AWSKinesisClient()) with PutRecordAction
  }
  def apply(awsCredentialsProvider: AWSCredentialsProvider): AmazonKinesisClient = {
    new AmazonKinesisClient(new AWSKinesisClient(awsCredentialsProvider)) with PutRecordAction
  }
  def apply(clientConfiguration: ClientConfiguration): AmazonKinesisClient = {
    new AmazonKinesisClient(new AWSKinesisClient(clientConfiguration)) with PutRecordAction
  }
  def apply(awsCredentialsProvider: AWSCredentialsProvider, clientConfiguration: ClientConfiguration): AmazonKinesisClient = {
    new AmazonKinesisClient(new AWSKinesisClient(awsCredentialsProvider, clientConfiguration)) with PutRecordAction
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
