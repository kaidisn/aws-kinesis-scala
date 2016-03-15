package jp.co.bizreach.kinesis

import java.util.concurrent.{TimeUnit, Executors}

import com.amazonaws.ClientConfiguration
import com.amazonaws.auth.AWSCredentialsProvider
import com.amazonaws.regions.Region

object BufferedAmazonKinesisClient {
  def apply(amount: Int, interval: Long)(implicit region: Region): BufferedAmazonKinesisClient = {
    new BufferedAmazonKinesisClient(AmazonKinesisClient(), amount, interval)
  }
  def apply(awsCredentialsProvider: AWSCredentialsProvider, amount: Int, interval: Long)(implicit region: Region): BufferedAmazonKinesisClient = {
    new BufferedAmazonKinesisClient(AmazonKinesisClient(awsCredentialsProvider), amount, interval)
  }
  def apply(clientConfiguration: ClientConfiguration, amount: Int, interval: Long)(implicit region: Region): BufferedAmazonKinesisClient = {
    new BufferedAmazonKinesisClient(AmazonKinesisClient(clientConfiguration), amount, interval)
  }
  def apply(awsCredentialsProvider: AWSCredentialsProvider, clientConfiguration: ClientConfiguration, amount: Int, interval: Long)(implicit region: Region): BufferedAmazonKinesisClient = {
    new BufferedAmazonKinesisClient(AmazonKinesisClient(awsCredentialsProvider, clientConfiguration), amount, interval)
  }
}

// TODO Would like to provide DiskBufferClient also
class BufferedAmazonKinesisClient(client: AmazonKinesisClient, amount: Int, interval: Long) {

  private val queue = new java.util.concurrent.ConcurrentLinkedQueue[Any]

  private val scheduler = Executors.newSingleThreadScheduledExecutor()
  scheduler.scheduleAtFixedRate(new BufferedKinesisSendTask(), 0, interval, TimeUnit.MILLISECONDS)

  def putRecord(request: PutRecordRequest): Unit = queue.add(request)

  def putRecords(request: PutRecordsRequest): Unit = queue.add(request)

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
        val requests = for(i <- 1 to amount if queue.size() != 0) yield queue.poll()
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