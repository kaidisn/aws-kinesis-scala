package jp.co.bizreach.kinesis.spark

import com.amazonaws.auth._
import org.scalatest._

class SparkAWSCredentialsSpec extends FunSuite with Matchers {

  test("DefaultAWSCredentialsProviderChain"){
    val credentials = DefaultCredentials

    credentials.provider shouldBe a[DefaultAWSCredentialsProviderChain]
  }

  test("AWSStaticCredentialsProvider"){
    val credentials = BasicCredentials("foo", "bar")

    credentials.provider shouldBe a[AWSStaticCredentialsProvider]
    assert(credentials.provider.getCredentials.getAWSAccessKeyId == "foo")
    assert(credentials.provider.getCredentials.getAWSSecretKey == "bar")
  }

  test("STSAssumeRoleSessionCredentialsProvider"){
    val credentials = STSCredentials("foo", "bar", Some("external"))

    credentials.provider shouldBe a[STSAssumeRoleSessionCredentialsProvider]
  }
}
