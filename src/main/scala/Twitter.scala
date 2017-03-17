import java.text.SimpleDateFormat

import org.apache.hadoop.fs.Path
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.StreamingContext._
import twitter4j.Status
import twitter4j.auth.OAuthAuthorization
import twitter4j.conf.ConfigurationBuilder

object Twitter {
  val config = new SparkConf().setAppName("twitter-stream-sentiment")
  val sc = new SparkContext(config)
  sc.setLogLevel("WARN")

  val ssc = new StreamingContext(sc, Minutes(1))
  val cb = new ConfigurationBuilder
  val pa = new Path("sad")
  cb.setDebugEnabled(true)
    .setOAuthConsumerKey("SN0ZW4jnN6dyz2ouQUbbO0J7n")
    .setOAuthConsumerSecret("SUkHN7gMK2Yqaksoem0bwdDs02EH881pJvV5BLAbqjTHSyUjIi")
    .setOAuthAccessToken("3459651314-AVGZON8ruVIdM0hOu9a1S2Nje9uquEzmsmiVQY0")
    .setOAuthAccessTokenSecret("bYcuKxOPJgh8FMxNaF894K8zgbGQNtZsiuJvaD4Nqiwjy")
  val auth = new OAuthAuthorization(cb.build())

  val dateFormat = new SimpleDateFormat("hh:mm:ss")

  val tweets = TwitterUtils.createStream(ssc, Some(auth), Array("SEGA", "NAMCO", "BANDAI"))
  val status = tweets.map(_.getText)
  val tags = stream.flatMap { status =>
    status.getHashtagEntities.map(_.getText)
  }
  tags.countByValue()
    .foreachRDD { rdd =>
      val now = org.joda.time.DateTime.now()
      rdd
        .sortBy(_._2)
        .map(x => (x, now))
        .saveAsTextFile(s"~/twitter/$now")
    }
  val tweets = stream.filter {t =>
    val tags = t.getText.split(" ").filter(_.startsWith("#")).map(_.toLowerCase)
    tags.contains("#bigdata") && tags.contains("#food")
  }
  def detectSentiment(message: String): SENTIMENT_TYPE

    ssc.start
    ssc.awaitTermination
}
