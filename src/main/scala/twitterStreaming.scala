import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.twitter.TwitterUtils

object twitterStreaming extends App{
  println("twitter Streaming is starting now...")

  val appName = "spark-streaming"
  val master = "local[*]"
  val conf = new SparkConf().setAppName(appName).setMaster(master)
  val ssc = new StreamingContext(conf, Seconds(10))

  val rootLogger = Logger.getRootLogger()
  rootLogger.setLevel(Level.ERROR)

  val consumerKey = "key"
  val consumerSecret = "key"
  val accessToken = "key"
  val accessTokenSecret = "key"

  System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
  System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
  System.setProperty("twitter4j.oauth.accessToken", accessToken)
  System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)

  val stream = TwitterUtils.createStream(ssc, None)


  val text = stream.map(_.getText)

  val hashTags = text.flatMap(_.split(" ")).filter(_.startsWith("#"))

  //  val hashTagCounts = hashTags.map(h => (h, 1)).reduceByKey(_+_)
  val hashTagCounts = hashTags.map(h => (h, 1)).reduceByKeyAndWindow(_+_, Seconds(60)) // Window 사용 : 60초의 데이터를 모음

  //  hashTagCounts.print() // 순서대로 정렬은 안됨..! foreachRDD를 사용하자

  hashTagCounts.foreachRDD {
    rdd =>
      println("=====")
      rdd.sortBy(_._2, false).take(10).foreach(println)
  }

  ssc.start()
  ssc.awaitTermination()
}

