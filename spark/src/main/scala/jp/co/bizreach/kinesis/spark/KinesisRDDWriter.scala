package jp.co.bizreach.kinesis.spark

import jp.co.bizreach.kinesis._
import org.apache.commons.codec.digest.DigestUtils
import org.apache.spark.TaskContext
import org.json4s.jackson.JsonMethods
import org.json4s.{Extraction, Formats, DefaultFormats}

class KinesisRDDWriter[A <: AnyRef](client: AmazonKinesisClient,
                                    streamName: String, chunk: Int) extends Serializable {

  val write = (task: TaskContext, data: Iterator[A]) => {
    // send data, including retry
    def put(a: Seq[PutRecordsEntry]) = client.putRecordsWithRetry(PutRecordsRequest(streamName, a))
      .left.getOrElse(Nil)
      .map(e => e._1 -> e._2.errorCode)

    val errors = data.foldLeft(
      (Nil: Seq[PutRecordsEntry], Nil: Seq[(PutRecordsEntry, String)])
    ){ (z, x) =>
      val (records, failed) = z
      val payload = serialize(x)
      val entry   = PutRecordsEntry(DigestUtils.sha256Hex(payload), payload)

      // record exceeds max size
      if (entry.recordSize > recordMaxDataSize)
        records -> ((entry -> "per-record size limit") +: failed)

      // execute
      else if (records.size >= chunk || (records.map(_.recordSize).sum + entry.recordSize) >= recordsMaxDataSize)
        (entry +: Nil) -> (put(records) ++ failed)

      // buffering
      else
        (entry +: records) -> failed
    } match {
      case (Nil, e)  => e
      case (rest, e) => put(rest) ++ e
    }

    // could not put record
    if (errors.nonEmpty) dump(errors)
  }

  // TODO
  protected def dump(failed: Seq[(PutRecordsEntry, String)]): Unit = ???

  protected def serialize(a: A)(implicit formats: Formats = DefaultFormats): Array[Byte] =
    JsonMethods.mapper.writeValueAsBytes(Extraction.decompose(a)(formats))

}
