package jp.co.bizreach.kinesis

import java.util.concurrent.{TimeUnit, Executors}

import scala.collection.mutable.Queue
import com.amazonaws.ClientConfiguration
import com.amazonaws.auth.AWSCredentialsProvider

object BufferedAmazonKinesisClient {
  def apply(amount: Int, interval: Long): BufferedAmazonKinesisClient = {
    new BufferedAmazonKinesisClient(new AmazonKinesisClient(), amount, interval)
  }
  def apply(awsCredentialsProvider: AWSCredentialsProvider, amount: Int, interval: Long): BufferedAmazonKinesisClient = {
    new BufferedAmazonKinesisClient(new AmazonKinesisClient(awsCredentialsProvider), amount, interval)
  }
  def apply(clientConfiguration: ClientConfiguration, amount: Int, interval: Long): BufferedAmazonKinesisClient = {
    new BufferedAmazonKinesisClient(new AmazonKinesisClient(clientConfiguration), amount, interval)
  }
  def apply(awsCredentialsProvider: AWSCredentialsProvider, clientConfiguration: ClientConfiguration, amount: Int, interval: Long): BufferedAmazonKinesisClient = {
    new BufferedAmazonKinesisClient(new AmazonKinesisClient(awsCredentialsProvider, clientConfiguration), amount, interval)
  }
}

class BufferedAmazonKinesisClient(client: AmazonKinesisClient, amount: Int, interval: Long) {

  private val queue = new Queue[Any]

  private val scheduler = Executors.newSingleThreadScheduledExecutor()
  scheduler.scheduleAtFixedRate(new BufferedKinesisSendTask(), 0, interval, TimeUnit.MILLISECONDS)

  def putRecord(request: PutRecordRequest): Unit = {
    queue += request
  }

  def putRecords(request: PutRecordsRequest): Unit = {
    queue += request
  }

  def shutdown(): Unit = {
    scheduler.shutdownNow()
    client.shutdown()
  }

  /**
   * Override to handle error in BufferedKinesisSendTask.
   * This implementation prints stacktrace simply.
   */
  def error(e: Exception): Unit = {
    e.printStackTrace()
  }

  private class BufferedKinesisSendTask extends Runnable {

    override def run(): Unit = {
      try {
        val requests = for(i <- 1 to amount if queue.nonEmpty) yield queue.dequeue()
        requests.foreach {
          case r: PutRecordRequest  => client.putRecord(r)
          case r: PutRecordsRequest => client.putRecords(r)
        }
      } catch {
        case e: Exception => error(e)
      }
    }
  }

}