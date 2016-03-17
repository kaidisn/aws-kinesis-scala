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

  /**
   * Writes a single data record from a producer into an Amazon Kinesis stream.
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
   * Writes a single data record from a producer into an Amazon Kinesis stream.
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
   * Writes multiple data records from a producer into an Amazon Kinesis stream in a single call.
   * The response Records sequence always includes the same number of records as the request sequence
   * by same ordering.
   *
   * Note: This method does not perform retry processing.
   *
   * @param request Represents the input for PutRecords
   * @return each record result. Right is result of the PutRecords operation on success.
   *         Left is the occurred error on failure.
   */
  def putRecords(request: PutRecordsRequest): Seq[Either[PutRecordsResultEntry, PutRecordsResultEntry]] = {
    withPutsRetry(request.records, 0){ entry => // not retry
      client.putRecords(PutRecordsRequest(request.streamName, entry))
    }
  }

  /**
   * Writes multiple data records from a producer into an Amazon Kinesis stream in a single call.
   * The response Records sequence always includes the same number of records as the request sequence
   * by same ordering.
   *
   * Note: This method does perform retry processing. Max retry count is 3 (SDK default).
   *
   * @param request Represents the input for PutRecords
   * @return each record result. Right is result of the PutRecords operation on success.
   *         Left is the occurred error on failure.
   */
  def putRecordsWithRetry(request: PutRecordsRequest): Seq[Either[PutRecordsResultEntry, PutRecordsResultEntry]] = {
    withPutsRetry(request.records){ entry =>
      client.putRecords(PutRecordsRequest(request.streamName, entry))
    }
  }

  def shutdown(): Unit = {
    client.shutdown()
  }
}
