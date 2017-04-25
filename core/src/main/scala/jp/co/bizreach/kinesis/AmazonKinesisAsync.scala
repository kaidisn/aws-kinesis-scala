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
import jp.co.bizreach.kinesis.action.PutRecordAction

import scala.concurrent._

object AmazonKinesisAsync {
  def apply()(implicit region: Regions): AmazonKinesisAsync = {
    new AmazonKinesisAsync(AmazonKinesisAsyncClientBuilder.standard
      .withRegion(region)
      .build()) with PutRecordAction
  }
  def apply(credentials: AWSCredentialsProvider)(implicit region: Regions): AmazonKinesisAsync = {
    new AmazonKinesisAsync(AmazonKinesisAsyncClientBuilder.standard
      .withCredentials(credentials)
      .withRegion(region)
      .build()) with PutRecordAction
  }
  def apply(config: ClientConfiguration)(implicit region: Regions): AmazonKinesisAsync = {
    new AmazonKinesisAsync(AmazonKinesisAsyncClientBuilder.standard
      .withClientConfiguration(config)
      .withRegion(region)
      .build()) with PutRecordAction
  }
  def apply(credentials: AWSCredentialsProvider, config: ClientConfiguration)(implicit region: Regions): AmazonKinesisAsync = {
    new AmazonKinesisAsync(AmazonKinesisAsyncClientBuilder.standard
      .withCredentials(credentials)
      .withClientConfiguration(config)
      .withRegion(region)
      .build()) with PutRecordAction
  }
  def apply(client: AWSKinesisAsync): AmazonKinesisAsync = {
    new AmazonKinesisAsync(client) with PutRecordAction
  }
}

class AmazonKinesisAsync(client: AWSKinesisAsync) {
  self: PutRecordAction =>

  /**
   * Writes a single data record from a producer into an Amazon Kinesis stream.
   *
   * Note: This method does not perform retry processing.
   * the result of the PutRecord operation returned by the service.
   *
   * @param request Represents the input for PutRecord
   * @return a future that will be completed with the result of the PutRecord operation
   */
  def putRecordAsync(request: PutRecordRequest): Future[PutRecordResult] = {
    val p = Promise[PutRecordResult]
    client.putRecordAsync(request, new AsyncHandler[AWSPutRecordRequest, AWSPutRecordResult]{
      override def onError(e: Exception): Unit = p.failure(e)
      override def onSuccess(req: AWSPutRecordRequest, res: AWSPutRecordResult): Unit = p.success(res)
    })
    p.future
  }

  /**
   * Writes a single data record from a producer into an Amazon Kinesis stream.
   *
   * Note: This method does perform retry processing. Max retry count is 3 (SDK default).
   *
   * @param request Represents the input for PutRecord
   * @param ec Retry processing always runs in the this implicit `ExecutionContext`
   * @return a future that will be completed with the result of the PutRecord operation
   */
  def putRecordAsyncWithRetry(request: PutRecordRequest)
                             (implicit ec: ExecutionContext): Future[PutRecordResult] = {
    withPutAsyncRetry(){
      val p = Promise[PutRecordResult]
      client.putRecordAsync(request, new AsyncHandler[AWSPutRecordRequest, AWSPutRecordResult]{
        override def onError(e: Exception): Unit = p.failure(e)
        override def onSuccess(req: AWSPutRecordRequest, res: AWSPutRecordResult): Unit = p.success(res)
      })
      p.future
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
   * @return a future that will be completed with each record result.
   *         Right is result of the PutRecords operation on success.
   *         Left is the occurred error on failure.
   */
  def putRecordsAsync(request: PutRecordsRequest): Future[Seq[Either[PutRecordsResultEntry, PutRecordsResultEntry]]] = {
    val p = Promise[Seq[Either[PutRecordsResultEntry, PutRecordsResultEntry]]]
    client.putRecordsAsync(request, new AsyncHandler[AWSPutRecordsRequest, AWSPutRecordsResult]{
      override def onError(e: Exception): Unit = p.failure(e)
      override def onSuccess(req: AWSPutRecordsRequest, res: AWSPutRecordsResult): Unit = p.success(
        (res: PutRecordsResult).records.map {
          case entry if Option(entry.errorCode).isEmpty => Right(entry)
          case entry => Left(entry)
        }
      )
    })
    p.future
  }

  /**
   * Writes multiple data records from a producer into an Amazon Kinesis stream in a single call.
   * The response Records sequence always includes the same number of records as the request sequence
   * by same ordering.
   *
   * Note: This method does perform retry processing. Max retry count is 3 (SDK default).
   *
   * @param request Represents the input for PutRecords
   * @param ec Retry processing always runs in the this implicit `ExecutionContext`
   * @return a future that will be completed with each record result.
   *         Right is result of the PutRecords operation on success.
   *         Left is the occurred error on failure.
   */
  def putRecordsAsyncWithRetry(request: PutRecordsRequest)
                              (implicit ec: ExecutionContext): Future[Seq[Either[PutRecordsResultEntry, PutRecordsResultEntry]]] = {
    withPutsAsyncRetry(request.records){ entry =>
      val p = Promise[PutRecordsResult]
      client.putRecordsAsync(PutRecordsRequest(request.streamName, entry), new AsyncHandler[AWSPutRecordsRequest, AWSPutRecordsResult]{
        override def onError(e: Exception): Unit = p.failure(e)
        override def onSuccess(req: AWSPutRecordsRequest, res: AWSPutRecordsResult): Unit = p.success(res)
      })
      p.future
    }
  }

  def shutdown(): Unit = {
    client.shutdown()
  }
}
