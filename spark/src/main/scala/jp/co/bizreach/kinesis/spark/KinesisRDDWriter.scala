package jp.co.bizreach.kinesis.spark

import com.amazonaws.auth.AWSCredentialsProvider
import com.amazonaws.regions.Regions
import jp.co.bizreach.kinesis._
import org.apache.commons.codec.digest.DigestUtils
import org.apache.spark.TaskContext
import org.json4s.jackson.JsonMethods
import org.json4s.{Extraction, Formats, DefaultFormats}
import org.slf4j.LoggerFactory

class KinesisRDDWriter[A <: AnyRef](streamName: String, region: Regions,
                                    credentials: Class[_ <: AWSCredentialsProvider],
                                    chunk: Int, client: Option[AmazonKinesis]) extends Serializable {
  private val logger = LoggerFactory.getLogger(getClass)

  val write = (task: TaskContext, data: Iterator[A]) => {
    // send data, including retry
    def put(a: Seq[PutRecordsEntry]) = client.getOrElse(KinesisRDDWriter.client(credentials)(region))
      .putRecordsWithRetry(PutRecordsRequest(streamName, a))
      .zipWithIndex.collect { case (Left(e), i) => a(i) -> s"${e.errorCode}: ${e.errorMessage}" }

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

    // failed records
    if (errors.nonEmpty) dump(errors)
  }

  protected def dump(errors: Seq[(PutRecordsEntry, String)]): Unit =
    logger.error(
      s"""Could not put record, count: ${errors.size}, following details:
         |${errors map { case (entry, message) => message + "\n" + new String(entry.data, "UTF-8") } mkString "\n"}
       """.stripMargin)

  protected def serialize(a: A)(implicit formats: Formats = DefaultFormats): Array[Byte] =
    JsonMethods.mapper.writeValueAsBytes(Extraction.decompose(a)(formats))

}

object KinesisRDDWriter {
  private val cache = collection.concurrent.TrieMap.empty[Regions, AmazonKinesis]

  private val client: Class[_ <: AWSCredentialsProvider] => Regions => AmazonKinesis = {
    credentials => implicit region =>
      cache.getOrElseUpdate(region, AmazonKinesis(credentials.getConstructor().newInstance()))
  }

}
