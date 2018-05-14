package jp.co.bizreach.kinesisfirehose.action

import com.amazonaws.services.kinesisfirehose.model.ServiceUnavailableException
import jp.co.bizreach.kinesisfirehose._
import org.scalatest._

class PutRecordActionSpec extends FunSuite {

  private def fixture = new {
    var retryCount = 0
  } with PutRecordAction {
    override protected def sleepDuration(retry: Int, retryLimit: Int): Long = {
      retryCount += 1
      0
    }
  }

  private val batchData = Seq("data1".getBytes, "data2".getBytes)

  test("putRecord on success. returns Right"){
    val action = fixture
    val result = action.withPutRetry(){
      PutRecordResult(recordId = "CGojNMJq3ms")
    }

    assert(result.isRight)
    assert(action.retryCount == 0)
  }

  test("putRecord on failure. does perform retry and returns Left"){
    val action = fixture
    val result = action.withPutRetry(){
      throw new ServiceUnavailableException("Error!!")
    }

    assert(result.isLeft)
    assert(action.retryCount == 3)
  }

  test("putRecordBatch on success. returns Right"){
    val action = fixture
    val result = action.withPutBatchRetry(batchData){ _ =>
      PutRecordBatchResult(failedPutCount = 0, records = Seq(
        PutRecordBatchResponseEntry(recordId = "AJJBALlfiFN", errorCode = null, errorMessage = null),
        PutRecordBatchResponseEntry(recordId = "goGaFS919Mm", errorCode = null, errorMessage = null)
      ))
    }

    assert(result.size == 2)
    assert(result(0).right.exists(_.recordId == "AJJBALlfiFN"))
    assert(result(1).right.exists(_.recordId == "goGaFS919Mm"))
    assert(action.retryCount == 0)
  }

  test("putRecordBatch on failure. does perform retry and returns at least one Left"){
    val action = fixture
    val result = action.withPutBatchRetry(batchData){ entries =>
      PutRecordBatchResult(failedPutCount = 1, records = Seq(
        PutRecordBatchResponseEntry(recordId = null, errorCode = "ServiceUnavailable", errorMessage = "error"),
        PutRecordBatchResponseEntry(recordId = "goGaFS919Mm", errorCode = null, errorMessage = null)
      ).take(entries.size))
    }

    assert(result.size == 2)
    assert(result(0).left.exists(_.errorCode == "ServiceUnavailable"))
    assert(result(1).right.exists(_.recordId == "goGaFS919Mm"))
    assert(action.retryCount == 3)
  }

}
