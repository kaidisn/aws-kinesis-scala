package jp.co.bizreach.kinesis.action

import com.amazonaws.services.kinesis.model.ProvisionedThroughputExceededException
import jp.co.bizreach.kinesis._
import org.scalatest._

import scala.concurrent.Future
import scala.language.reflectiveCalls

class PutRecordActionSpec extends AsyncFunSuite {

  private def fixture = new {
    var retryCount = 0
  } with PutRecordAction {
    override protected def sleepDuration(retry: Int, retryLimit: Int): Long = {
      retryCount += 1
      0
    }
  }

  private val putRecordsData = Seq(
    PutRecordsEntry(partitionKey = "partitionKey-x", data = "data1".getBytes),
    PutRecordsEntry(partitionKey = "partitionKey-x", data = "data2".getBytes)
  )

  test("putRecord on success. returns Right"){
    val action = fixture
    val result = action.withPutRetry(){
      PutRecordResult(shardId = "shardId-000000000001", sequenceNumber = "123")
    }

    assert(result.isRight)
    assert(action.retryCount == 0)
  }

  test("putRecord on failure. does perform retry and returns Left"){
    val action = fixture
    val result = action.withPutRetry(){
      throw new ProvisionedThroughputExceededException("Error!!")
    }

    assert(result.isLeft)
    assert(action.retryCount == 3)
  }

  test("putRecords on success. returns Right"){
    val action = fixture
    val result = action.withPutsRetry(putRecordsData){ _ =>
      PutRecordsResult(failedRecordCount = 0, records = Seq(
        PutRecordsResultEntry(sequenceNumber = "123", shardId = "shardId-000000000001", errorCode = null, errorMessage = null),
        PutRecordsResultEntry(sequenceNumber = "456", shardId = "shardId-000000000001", errorCode = null, errorMessage = null)
      ))
    }

    assert(result.size == 2)
    assert(result(0).right.exists(_.sequenceNumber == "123"))
    assert(result(1).right.exists(_.sequenceNumber == "456"))
    assert(action.retryCount == 0)
  }

  test("putRecords on failure. does perform retry and returns at least one Left"){
    val action = fixture
    val result = action.withPutsRetry(putRecordsData){ entries =>
      PutRecordsResult(failedRecordCount = 1, records = Seq(
        PutRecordsResultEntry(sequenceNumber = null, shardId = null, errorCode = "ProvisionedThroughputExceededException", errorMessage = "error"),
        PutRecordsResultEntry(sequenceNumber = "456", shardId = "shardId-000000000001", errorCode = null, errorMessage = null)
      ).take(entries.size))
    }

    assert(result.size == 2)
    assert(result(0).left.exists(_.errorCode == "ProvisionedThroughputExceededException"))
    assert(result(1).right.exists(_.sequenceNumber == "456"))
    assert(action.retryCount == 3)
  }

  test("putRecordAsync on success. returns successful future"){
    val action = fixture
    val f = action.withPutAsyncRetry(){
      Future.successful(
        PutRecordResult(shardId = "shardId-000000000001", sequenceNumber = "123")
      )
    }

    f map { result =>
      assert(result.sequenceNumber == "123")
      assert(action.retryCount == 0)
    }
  }

  test("putRecordAsync on failure. does perform retry and returns failed future"){
    val action = fixture
    val f = action.withPutAsyncRetry(){
      Future.failed(
        new ProvisionedThroughputExceededException("Error!!")
      )
    }

    recoverToSucceededIf[ProvisionedThroughputExceededException](f) map { result =>
      assert(result == Succeeded)
      assert(action.retryCount == 3)
    }
  }

  test("putRecordsAsync on success. returns Right"){
    val action = fixture
    val f = action.withPutsAsyncRetry(putRecordsData){ _ =>
      Future.successful(
        PutRecordsResult(failedRecordCount = 0, records = Seq(
          PutRecordsResultEntry(sequenceNumber = "123", shardId = "shardId-000000000001", errorCode = null, errorMessage = null),
          PutRecordsResultEntry(sequenceNumber = "456", shardId = "shardId-000000000001", errorCode = null, errorMessage = null)
        ))
      )
    }

    f map { result =>
      assert(result.size == 2)
      assert(result(0).right.exists(_.sequenceNumber == "123"))
      assert(result(1).right.exists(_.sequenceNumber == "456"))
      assert(action.retryCount == 0)
    }
  }

  test("putRecordsAsync on failure. does perform retry and returns at least one Left"){
    val action = fixture
    val f = action.withPutsAsyncRetry(putRecordsData){ entries =>
      Future.successful(
        PutRecordsResult(failedRecordCount = 1, records = Seq(
          PutRecordsResultEntry(sequenceNumber = null, shardId = null, errorCode = "ProvisionedThroughputExceededException", errorMessage = "error"),
          PutRecordsResultEntry(sequenceNumber = "456", shardId = "shardId-000000000001", errorCode = null, errorMessage = null)
        ).take(entries.size))
      )
    }

    f map { result =>
      assert(result.size == 2)
      assert(result(0).left.exists(_.errorCode == "ProvisionedThroughputExceededException"))
      assert(result(1).right.exists(_.sequenceNumber == "456"))
      assert(action.retryCount == 3)
    }
  }

}
