package jp.co.bizreach.kinesisfirehose.action

import com.amazonaws.retry.PredefinedRetryPolicies.DEFAULT_MAX_ERROR_RETRY
import com.amazonaws.services.kinesisfirehose.model.ServiceUnavailableException

import jp.co.bizreach.kinesisfirehose._
import org.slf4j.LoggerFactory

import scala.annotation.tailrec
import scala.collection.mutable.ArrayBuffer
import scala.math._
import scala.util.Random

trait PutRecordAction {

  private val logger = LoggerFactory.getLogger(getClass)

  def withPutBatchRetry(records: Seq[Array[Byte]], retryLimit: Int = DEFAULT_MAX_ERROR_RETRY)
                       (f: Seq[Array[Byte]] => PutRecordBatchResult): Seq[Either[PutRecordBatchResponseEntry, PutRecordBatchResponseEntry]] = {

    val buffer = ArrayBuffer[Either[PutRecordBatchResponseEntry, PutRecordBatchResponseEntry]](Nil.padTo(records.size, null): _*)

    @tailrec
    def put0(records: Seq[(Array[Byte], Int)], retry: Int = 0): Unit = {
      val result = f(records.map(_._1))

      val failed = records zip result.records flatMap {
        case ((_, i), entry) if Option(entry.errorCode).isEmpty =>
          buffer(i) = Right(entry)
          None
        case ((record, i), entry) =>
          buffer(i) = Left(entry)
          Some(record -> i)
      }

      // success, or exceed the upper limit of the retry
      if (failed.isEmpty || retry >= retryLimit) ()
      // retry
      else {
        Thread.sleep(sleepDuration(retry, retryLimit))
        logger.warn(s"Retrying the put requests. Retry count: ${retry + 1}")
        put0(failed, retry + 1)
      }
    }

    put0(records.zipWithIndex)
    buffer.toList
  }

  def withPutRetry(retryLimit: Int = DEFAULT_MAX_ERROR_RETRY)
                  (f: => PutRecordResult): Either[Throwable, PutRecordResult] = {
    @tailrec
    def put0(retry: Int = 0): Either[Throwable, PutRecordResult] = {
      try
        Right(f)
      catch {
        case e: ServiceUnavailableException => if (retry >= retryLimit) Left(e) else {
          Thread.sleep(sleepDuration(retry, retryLimit))
          logger.warn(s"Retrying the put request. Retry count: ${retry + 1}")
          put0(retry + 1)
        }
      }
    }

    put0()
  }

  protected def sleepDuration(retry: Int, retryLimit: Int): Long = {
    // scaling factor
    val d = 0.5 + Random.nextDouble() * 0.1
    // possible seconds
    val durations = (0 until retryLimit).map(n => pow(2, n) * d)

    (durations(retry) * 1000).toLong
  }

}
