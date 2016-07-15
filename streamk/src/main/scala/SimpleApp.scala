package net.jacob

import org.apache.spark.SparkConf
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.storage.StorageLevel._
import org.apache.spark.serializer.KryoRegistrator
import java.io.File
import com.datastax.spark.connector.cql.CassandraConnector
//import com.twitter.bijection.Conversion.asMethod
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.sql._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import collection.mutable.ListBuffer
import com.datastax.spark.connector.streaming._
import com.datastax.spark.connector._
import com.datastax.spark.connector.writer._
import net.jacob.avro.Tweet

import com.typesafe.config.ConfigFactory
import kafka.serializer.{DefaultDecoder, StringDecoder}
import com.twitter.bijection.avro.SpecificAvroCodecs
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import com.twitter.bijection.avro.GenericAvroCodecs
// must pass cassandra ip, zookeeper ip
//grab tweets from kafka stream and saves them to our "data lake" in cassandra

object SimpleApp{
  def main(args : Array[String]): Unit ={
    //set up spark configuration and connect to cassandra
    val conf = new SparkConf()
      .setMaster("local[4]")
      .setAppName("SimpleApp")
      .set("spark.cassandra.connection.host", args(0))
    val sc = new SparkContext(conf)//generate spark context

    val intervalSecs = 2//wait two seconds then check stream
    val ssc = new StreamingContext(sc, Seconds(intervalSecs))//generate spark streaming context

    val kafkaIP = args(1)+":2181"
    val encTweets = {//generate dstream with kafka data
      val topics = Map("tweets" -> 1)//topic this consumer group will consume
      val kafkaParams = Map(//paramaters for kafka dstream
        "zookeeper.connect" -> kafkaIP,//location of zookeeper
        "auto.offset.reset" -> "smallest",//automatically reset the offset to the smallest offset if Zookeeper is out of range
        "group.id" -> "stream")//set group id to stream so won't collide with speed
      KafkaUtils.createStream[String, Array[Byte], StringDecoder, DefaultDecoder](
        ssc, kafkaParams, topics, StorageLevel.MEMORY_ONLY)
    }
    val tweets = encTweets.flatMap(x => SpecificAvroCodecs.toBinary[Tweet].invert(x._2).toOption)
    //convert kafka data to avro object with the avro Tweet schema applied to it
    val createKeyspace = "CREATE KEYSPACE IF NOT EXISTS twitter WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1 };"
    val createTable = "CREATE TABLE IF NOT EXISTS twitter.streamtable(id bigint PRIMARY KEY, user text,tweettext text, favorite boolean, isretweet boolean, retweeted boolean, sensitive boolean,date timestamp);"
    //getId,(boolean),(boolean),(boolean),,
    // getUser.getScreenName,
    val cassandraConnector = CassandraConnector(conf)

    cassandraConnector.withSessionDo { session =>
      session.execute(createKeyspace)//create keyspace if does not exits
      session.execute(createTable)//create stream table if does not exist
    }

    tweets.map{j => (j.getId,j.getUser.toString,j.getText.toString,j.getFavorite.toString.toBoolean,
      j.getIsRetweet.toString.toBoolean,j.getRetweeted.toString.toBoolean,j.getSensitive.toString.toBoolean,
      new java.util.Date())}.saveToCassandra("twitter", "streamtable",
      SomeColumns("id","user", "tweettext","favorite","isretweet","retweeted","sensitive","date"))
      //map each avro object(tweet and its meta data) grab relevant information and save to Cassandra
    ssc.start()//start spark streaming
    ssc.awaitTermination()//spark streaming will wait to stop until you tell it to

  }
}