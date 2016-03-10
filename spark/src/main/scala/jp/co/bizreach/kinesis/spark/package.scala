package jp.co.bizreach.kinesis

import org.apache.spark.rdd.RDD

package object spark {

  implicit class RichRDD[A](rdd: RDD[A]) {
    def saveToKinesis(): Unit = ???
  }

}
