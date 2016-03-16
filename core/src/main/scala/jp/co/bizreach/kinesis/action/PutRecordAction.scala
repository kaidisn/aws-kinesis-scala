package jp.co.bizreach.kinesis.action

import com.amazonaws.retry.PredefinedRetryPolicies.DEFAULT_MAX_ERROR_RETRY
import com.amazonaws.services.kinesis.model.ProvisionedThroughputExceededException

import jp.co.bizreach.kinesis._
import org.slf4j.LoggerFactory

import scala.annotation.tailrec
import scala.math.pow
import scala.util.Random

trait PutRecordAction {

  private val logger = LoggerFactory.getLogger(getClass)

  protected val retryLimit = DEFAULT_MAX_ERROR_RETRY

  @tailrec
  final def withRetry(records: Seq[PutRecordsEntry], retry: Int = 0)
                     (f: Seq[PutRecordsEntry] => PutRecordsResult): Either[Seq[(PutRecordsEntry, PutRecordsResultEntry)], Unit] = {
    val result = f(records)

    result.failedRecordCount match {
      // success
      case 0 => Right(())

      // error: exceed the upper limit of the retry
      case _ if retry >= retryLimit => Left(result.records.zipWithIndex.collect {
        case (entry, i) if Option(entry.errorCode).isDefined => records(i) -> entry
      })

      // retry
      case _ =>
        Thread.sleep(sleepDuration(retry))
        logger.warn(s"Retrying to put records. Retry count: ${retry + 1}")
        withRetry(
          records = result.records.zipWithIndex.collect {
            case (entry, i) if Option(entry.errorCode).isDefined => records(i)
          },
          retry = retry + 1
        )(f)
    }
  }

  @tailrec
  final def withRetry(retry: Int)(f: => PutRecordResult): Either[Throwable, PutRecordResult] = {
    try
      Right(f)
    catch {
      case e: ProvisionedThroughputExceededException => if (retry >= retryLimit) Left(e) else {
        Thread.sleep(sleepDuration(retry))
        logger.warn(s"Retrying to put records. Retry count: ${retry + 1}")
        withRetry(retry + 1)(f)
      }
    }
  }

  protected def sleepDuration(retry: Int): Long = {
    // scaling factor
    val d = 0.5 + Random.nextDouble() * 0.1
    // possible seconds
    val durations = (0 until retryLimit).map(n => pow(2, n) * d)

    (durations(retry) * 1000).toLong
  }

}
