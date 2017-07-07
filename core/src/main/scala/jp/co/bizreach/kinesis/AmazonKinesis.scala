package jp.co.bizreach.kinesis

import com.amazonaws.ClientConfiguration
import com.amazonaws.auth.AWSCredentialsProvider
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.regions.Regions
import com.amazonaws.services.kinesis.{AmazonKinesisClientBuilder, AmazonKinesis => AWSKinesis}
import jp.co.bizreach.kinesis.action.PutRecordAction

object AmazonKinesis {
  def apply()(implicit region: Regions): AmazonKinesis = {
    new AmazonKinesis(AmazonKinesisClientBuilder.standard
      .withRegion(region)
      .build()) with PutRecordAction
  }
  def apply(credentials: AWSCredentialsProvider)(implicit region: Regions): AmazonKinesis = {
    new AmazonKinesis(AmazonKinesisClientBuilder.standard
      .withCredentials(credentials)
      .withRegion(region)
      .build()) with PutRecordAction
  }
  def apply(endpointConfiguration: EndpointConfiguration): AmazonKinesis = {
    new AmazonKinesis(AmazonKinesisClientBuilder.standard
      .withEndpointConfiguration(endpointConfiguration)
      .build()) with PutRecordAction
  }
  def apply(config: ClientConfiguration)(implicit region: Regions): AmazonKinesis = {
    new AmazonKinesis(AmazonKinesisClientBuilder.standard
      .withClientConfiguration(config)
      .withRegion(region)
      .build()) with PutRecordAction
  }
  def apply(config: ClientConfiguration, endpointConfiguration: EndpointConfiguration): AmazonKinesis = {
    new AmazonKinesis(AmazonKinesisClientBuilder.standard
      .withClientConfiguration(config)
      .withEndpointConfiguration(endpointConfiguration)
      .build()) with PutRecordAction
  }
  def apply(credentials: AWSCredentialsProvider, endpointConfiguration: EndpointConfiguration): AmazonKinesis = {
    new AmazonKinesis(AmazonKinesisClientBuilder.standard
      .withCredentials(credentials)
      .withEndpointConfiguration(endpointConfiguration)
      .build()) with PutRecordAction
  }
  def apply(credentials: AWSCredentialsProvider, config: ClientConfiguration)(implicit region: Regions): AmazonKinesis = {
    new AmazonKinesis(AmazonKinesisClientBuilder.standard
      .withCredentials(credentials)
      .withClientConfiguration(config)
      .withRegion(region)
      .build()) with PutRecordAction
  }
  def apply(credentials: AWSCredentialsProvider, config: ClientConfiguration, endpointConfiguration: EndpointConfiguration): AmazonKinesis = {
    new AmazonKinesis(AmazonKinesisClientBuilder.standard
      .withCredentials(credentials)
      .withClientConfiguration(config)
      .withEndpointConfiguration(endpointConfiguration)
      .build()) with PutRecordAction
  }
  def apply(client: AWSKinesis): AmazonKinesis = {
    new AmazonKinesis(client) with PutRecordAction
  }
}

class AmazonKinesis(client: AWSKinesis) extends Serializable{
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
