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
//speed layer of #trendOff grabs tweets from kafka stream combines user tweets
//and save user, combined tweet text and current date to cassandra table
object SimpleApp{
  def main(args : Array[String]): Unit ={
    //set up spark configuration with connection to cassandra
    val conf = new SparkConf()
      .setMaster("local[4]")
      .setAppName("SimpleApp")
      .set("spark.cassandra.connection.host", args(0))
    val sc = new SparkContext(conf)//generate spark context

    val intervalSecs = 2//check stream every two seconds
    val ssc = new StreamingContext(sc, Seconds(intervalSecs))//generate streaming context
    val kafkaIP = args(1)+":2181"
    val encTweets = {//create dstream with kafka stream data
      val topics = Map("tweets" -> 1)//topics this consumer group will consume
      val kafkaParams = Map(//parameters for kafka
        "zookeeper.connect" -> kafkaIP,//ip and port consumer will look for the stream data
        "auto.offset.reset" -> "smallest",//
        "group.id" -> "speed")//set group id so wont collide with stream layer
      KafkaUtils.createStream[String, Array[Byte], StringDecoder, DefaultDecoder](
        ssc, kafkaParams, topics, StorageLevel.MEMORY_ONLY)//create kafka dstream with specified parameters
    }
    val tweets = encTweets.flatMap(x => SpecificAvroCodecs.toBinary[Tweet].invert(x._2).toOption)//every data point
    //converted from avro binary with schema Tweet into avro object using twitter bijection class #1to1 #Onto
    
    val createTable = "CREATE TABLE IF NOT EXISTS twitter.speedtable(user text,tweettext text, date timestamp, PRIMARY KEY (user,date));"

    val cassandraConnector = CassandraConnector(conf)

    cassandraConnector.withSessionDo { session =>
      session.execute(createTable)//create table if does not exist
    }

    tweets.map{j => (j.getUser.toString,j.getText.toString)}.reduceByKey(_++_).
      map(i => (i._1, i._2, new java.util.Date())).saveToCassandra("twitter", "speedtable",
        SomeColumns("user", "tweettext", "date"))
    //reduce by user and combine their tweets into one string and then save user, combined string
    // and current date to cassandra
    ssc.start()//start spark streaming
    ssc.awaitTermination()//stream will wait to terminate

  }
}