package jp.co.bizreach.kinesis.spark

import com.amazonaws.auth._

sealed trait SparkAWSCredentials extends Serializable {
  // The AWS credentials will be discovered on the workers
  def provider: AWSCredentialsProvider
}

case object DefaultCredentials extends SparkAWSCredentials {
  def provider = new DefaultAWSCredentialsProviderChain
}

/**
 * Returns [[AWSStaticCredentialsProvider]] constructed using basic AWS keypair.
 *
 * @param accessKeyId AWS access key
 * @param secretAccessKey AWS secret access key
 */
case class BasicCredentials(accessKeyId: String, secretAccessKey: String) extends SparkAWSCredentials {
  def provider = new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessKeyId, secretAccessKey))
}

/**
 * Returns [[STSAssumeRoleSessionCredentialsProvider]] which assumes a role
 * in order to authenticate against resources in an external account.
 *
 * @param stsRoleArn arn of the role to be assumed
 * @param stsSessionName identifier for the assumed role session
 * @param stsExternalId external id used in the service call used to retrieve session credentials
 * @param longLivedCreds credentials provider used to generate sessions in the assumed role
 */
case class STSCredentials(stsRoleArn: String,
                          stsSessionName: String,
                          stsExternalId: Option[String] = None,
                          longLivedCreds: SparkAWSCredentials = DefaultCredentials) extends SparkAWSCredentials {
  def provider = {
    val builder = new STSAssumeRoleSessionCredentialsProvider.Builder(stsRoleArn, stsSessionName)
      .withLongLivedCredentialsProvider(longLivedCreds.provider)

    stsExternalId.map(builder.withExternalId).getOrElse(builder).build()
  }
}
