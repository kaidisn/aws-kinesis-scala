package jp.co.bizreach.kinesis

import com.amazonaws.ClientConfiguration
import com.amazonaws.auth.AWSCredentialsProvider

object AmazonKinesisClient {
  def apply(): AmazonKinesisClient = {
    new AmazonKinesisClient()
  }
  def apply(awsCredentialsProvider: AWSCredentialsProvider): AmazonKinesisClient = {
    new AmazonKinesisClient(awsCredentialsProvider)
  }
  def apply(clientConfiguration: ClientConfiguration): AmazonKinesisClient = {
    new AmazonKinesisClient(clientConfiguration)
  }
  def apply(awsCredentialsProvider: AWSCredentialsProvider, clientConfiguration: ClientConfiguration): AmazonKinesisClient = {
    new AmazonKinesisClient(awsCredentialsProvider, clientConfiguration)
  }
}
