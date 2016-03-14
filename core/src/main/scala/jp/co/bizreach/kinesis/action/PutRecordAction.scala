package jp.co.bizreach.kinesis.action

import com.amazonaws.services.kinesis.model.ProvisionedThroughputExceededException
import jp.co.bizreach.kinesis._

import scala.annotation.tailrec
import scala.math.pow
import scala.util.Random

trait PutRecordAction {

  protected val retryLimit = 3

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
//        log.warn sprintf('Retrying to put records. Retry count: %d', retry_count)
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
//        log.warn sprintf('Retrying to put records. Retry count: %d', retry_count)
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
