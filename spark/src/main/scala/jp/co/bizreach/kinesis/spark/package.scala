package jp.co.bizreach.kinesis

import com.amazonaws.auth.InstanceProfileCredentialsProvider
import com.amazonaws.regions.{Region, Regions}
import org.apache.spark.rdd.RDD

import scala.collection.mutable

package object spark {

  private val client = mutable.Map[Regions, AmazonKinesisClient]()

  implicit class RichRDD[A <: AnyRef](rdd: RDD[A]) {
    def saveToKinesis(streamName: String, region: Regions): Unit = if (!rdd.isEmpty) {
      rdd.sparkContext.runJob(rdd, new KinesisRDDWriter(
        client     = client.getOrElseUpdate(region,
          AmazonKinesisClient(new InstanceProfileCredentialsProvider())(Region.getRegion(region))),
        streamName = streamName,
        chunk      = 30
      ).write)
    }
  }

}
