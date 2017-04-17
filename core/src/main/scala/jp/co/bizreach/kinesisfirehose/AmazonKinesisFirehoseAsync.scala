package jp.co.bizreach.kinesisfirehose

import com.amazonaws.ClientConfiguration
import com.amazonaws.auth.AWSCredentialsProvider
import com.amazonaws.regions.Regions
import com.amazonaws.services.kinesisfirehose.{AmazonKinesisFirehoseAsync => AWSKinesisFirehoseAsync,
  AmazonKinesisFirehoseAsyncClientBuilder}

object AmazonKinesisFirehoseAsync {
  def apply()(implicit region: Regions): AmazonKinesisFirehoseAsync = {
    new AmazonKinesisFirehoseAsync(AmazonKinesisFirehoseAsyncClientBuilder.standard
      .withRegion(region)
      .build())
  }
  def apply(credentials: AWSCredentialsProvider)(implicit region: Regions): AmazonKinesisFirehoseAsync = {
    new AmazonKinesisFirehoseAsync(AmazonKinesisFirehoseAsyncClientBuilder.standard
      .withCredentials(credentials)
      .withRegion(region)
      .build())
  }
  def apply(config: ClientConfiguration)(implicit region: Regions): AmazonKinesisFirehoseAsync = {
    new AmazonKinesisFirehoseAsync(AmazonKinesisFirehoseAsyncClientBuilder.standard
      .withClientConfiguration(config)
      .withRegion(region)
      .build())
  }
  def apply(credentials: AWSCredentialsProvider, config: ClientConfiguration)(implicit region: Regions): AmazonKinesisFirehoseAsync = {
    new AmazonKinesisFirehoseAsync(AmazonKinesisFirehoseAsyncClientBuilder.standard
      .withCredentials(credentials)
      .withClientConfiguration(config)
      .withRegion(region)
      .build())
  }
  def apply(client: AWSKinesisFirehoseAsync): AmazonKinesisFirehoseAsync = {
    new AmazonKinesisFirehoseAsync(client)
  }
}

class AmazonKinesisFirehoseAsync(client: AWSKinesisFirehoseAsync) {

}
