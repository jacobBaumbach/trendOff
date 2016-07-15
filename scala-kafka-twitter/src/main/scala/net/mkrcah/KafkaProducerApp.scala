package net.mkrcah

import java.util.Properties

import com.twitter.bijection.avro.SpecificAvroCodecs.{toJson, toBinary}
import com.typesafe.config.ConfigFactory
import kafka.javaapi.producer.Producer
import kafka.producer.{KeyedMessage, ProducerConfig}
import net.mkrcah.TwitterStream.OnTweetPosted
import net.mkrcah.avro.Tweet
import twitter4j.{Status, FilterQuery}

object KafkaProducerApp {

  private val conf = ConfigFactory.load()//load twitter credentials and zookeeper and kafka ip and port

  val KafkaTopic = "tweets"//set topic name

  //generate kafka producer
  val kafkaProducer = {
    val props = new Properties()//generate new java properties
    props.put("metadata.broker.list", conf.getString("kafka.brokers"))//set ip and port of kafka brokers
    props.put("request.required.acks", "1")
    props.put("num.partitions","1")//number of partitions
    val config = new ProducerConfig(props)//set the kafka producer configuration with the properties we set
    new Producer[String, Array[Byte]](config)//generate new producer
  }

  val filterUsOnly = new FilterQuery().locations(Array(//filter for tweets that are in US
    Array(-126.562500,30.448674),
    Array(-61.171875,44.087585)))


  def main (args: Array[String]) {
    val twitterStream = TwitterStream.getStream//get Twitter stream using class created by mkrcah
    twitterStream.addListener(new OnTweetPosted(s => sendToKafka(toTweet(s))))//map each tweet 
    //send message and if exception send unit value
    twitterStream.filter(filterUsOnly)//filter so only tweets from US
  }

  private def toTweet(s: Status): Tweet = {
    new Tweet(s.getId(),s.getUser.getScreenName, s.getText,s.isFavorited.toString,s.isRetweet.toString,
      s.isRetweeted.toString,s.isPossiblySensitive.toString)//map tweet to our avro schema
  }

  private def sendToKafka(t:Tweet) {
    println(toJson(t.getSchema).apply(t))//print tweet
    val tweetEnc = toBinary[Tweet].apply(t)//encode in avro with Tweet schema
    //create Kafka message with topic and encoded tweet
    val msg = new KeyedMessage[String, Array[Byte]](KafkaTopic, tweetEnc)
    kafkaProducer.send(msg)//send the kafka message using our producer
  }

}



