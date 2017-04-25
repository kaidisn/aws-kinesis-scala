package jp.co.bizreach.kinesisfirehose

import com.amazonaws.ClientConfiguration
import com.amazonaws.auth.AWSCredentialsProvider
import com.amazonaws.regions.Regions
import com.amazonaws.services.kinesisfirehose.{AmazonKinesisFirehose => AWSKinesisFirehose,
  AmazonKinesisFirehoseClientBuilder}
import jp.co.bizreach.kinesisfirehose.action.PutRecordAction

object AmazonKinesisFirehose {
  def apply()(implicit region: Regions): AmazonKinesisFirehose = {
    new AmazonKinesisFirehose(AmazonKinesisFirehoseClientBuilder.standard
      .withRegion(region)
      .build()) with PutRecordAction
  }
  def apply(credentials: AWSCredentialsProvider)(implicit region: Regions): AmazonKinesisFirehose = {
    new AmazonKinesisFirehose(AmazonKinesisFirehoseClientBuilder.standard
      .withCredentials(credentials)
      .withRegion(region)
      .build()) with PutRecordAction
  }
  def apply(config: ClientConfiguration)(implicit region: Regions): AmazonKinesisFirehose = {
    new AmazonKinesisFirehose(AmazonKinesisFirehoseClientBuilder.standard
      .withClientConfiguration(config)
      .withRegion(region)
      .build()) with PutRecordAction
  }
  def apply(credentials: AWSCredentialsProvider, config: ClientConfiguration)(implicit region: Regions): AmazonKinesisFirehose = {
    new AmazonKinesisFirehose(AmazonKinesisFirehoseClientBuilder.standard
      .withCredentials(credentials)
      .withClientConfiguration(config)
      .withRegion(region)
      .build()) with PutRecordAction
  }
  def apply(client: AWSKinesisFirehose): AmazonKinesisFirehose = {
    new AmazonKinesisFirehose(client) with PutRecordAction
  }
}

class AmazonKinesisFirehose(client: AWSKinesisFirehose) {
  self: PutRecordAction =>

  /**
   * Writes a single data record into an Amazon Kinesis Firehose delivery stream.
   *
   * Note: This method does not perform retry processing.
   *
   * @param request Represents the input for PutRecord
   * @return Right is result of the PutRecord operation on success.
   *         Left is the occurred error on failure.
   */
  def putRecord(request: PutRecordRequest): Either[Throwable, PutRecordResult] = {
    withPutRetry(0){ // not retry
      client.putRecord(request)
    }
  }

  /**
   * Writes a single data record into an Amazon Kinesis Firehose delivery stream.
   *
   * Note: This method does perform retry processing. Max retry count is 3 (SDK default).
   *
   * @param request Represents the input for PutRecord
   * @return Right is result of the PutRecord operation on success.
   *         Left is the occurred error on failure.
   */
  def putRecordWithRetry(request: PutRecordRequest): Either[Throwable, PutRecordResult] = {
    withPutRetry(){
      client.putRecord(request)
    }
  }

  /**
   * Writes multiple data records into an Amazon Kinesis Firehose delivery stream in a single call.
   * The response Records sequence always includes the same number of records as the request sequence
   * by same ordering.
   *
   * Note: This method does not perform retry processing.
   *
   * @param request Represents the input for PutRecordBatch
   * @return each record result. Right is result of the PutRecordBatch operation on success.
   *         Left is the occurred error on failure.
   */
  def putRecordBatch(request: PutRecordBatchRequest): Seq[Either[PutRecordBatchResponseEntry, PutRecordBatchResponseEntry]] = {
    withPutBatchRetry(request.records, 0){ entry => // not retry
      client.putRecordBatch(PutRecordBatchRequest(request.deliveryStreamName, entry))
    }
  }

  /**
   * Writes multiple data records into an Amazon Kinesis Firehose delivery stream in a single call.
   * The response Records sequence always includes the same number of records as the request sequence
   * by same ordering.
   *
   * Note: This method does perform retry processing. Max retry count is 3 (SDK default).
   *
   * @param request Represents the input for PutRecordBatch
   * @return each record result. Right is result of the PutRecordBatch operation on success.
   *         Left is the occurred error on failure.
   */
  def putRecordBatchWithRetry(request: PutRecordBatchRequest): Seq[Either[PutRecordBatchResponseEntry, PutRecordBatchResponseEntry]] = {
    withPutBatchRetry(request.records){ entry =>
      client.putRecordBatch(PutRecordBatchRequest(request.deliveryStreamName, entry))
    }
  }

  def shutdown(): Unit = {
    client.shutdown()
  }

}
